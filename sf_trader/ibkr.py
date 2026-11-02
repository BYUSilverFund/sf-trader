"""Read-only access to TWS. This module must never submit, modify or cancel orders."""

import time

import dataframely as dy
import polars as pl
from ibapi.account_summary_tags import AccountSummaryTags
from ibapi.sync_wrapper import Contract, TWSSyncWrapper

from sf_trader.config import IBKRSettings
from sf_trader.models import Positions, Quotes

DELAYED_IF_NOT_SUBSCRIBED = 3
MAX_MARKET_DATA_LINES = 90
QUOTE_WAIT_SECONDS = 10.0
CONTRACT_WAIT_SECONDS = 10.0
RESPONSE_WAIT_SECONDS = 15.0
REQUEST_PACING_SECONDS = 0.025


def to_ibkr_symbol(ticker: str) -> str:
    """Class shares use a dot in our data and a space at IBKR: BRK.B is BRK B."""
    return ticker if ticker.endswith(".CVR") else ticker.replace(".", " ")


def from_ibkr_symbol(symbol: str) -> str:
    return symbol.replace(" ", ".")


def _stock_contract(ticker: str) -> Contract:
    contract = Contract()
    contract.symbol = to_ibkr_symbol(ticker)
    contract.secType = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"
    return contract


def _positive(*values: float | None) -> float | None:
    """First real price. IBKR sends -1 or 0 when it has none."""
    return next((value for value in values if value is not None and value > 0), None)


class IBKRClient:
    def __init__(self, settings: IBKRSettings) -> None:
        self._app = TWSSyncWrapper(timeout=30)
        self._next_req_id = 1
        if not self._app.connect_and_start(
            host=settings.host, port=settings.port, client_id=settings.client_id
        ):
            raise RuntimeError(
                f"Failed to connect to TWS at {settings.host}:{settings.port}"
            )
        self._wait_for("next_valid_id_0")
        self._app.reqMarketDataType(DELAYED_IF_NOT_SUBSCRIBED)

    def __enter__(self) -> "IBKRClient":
        return self

    def __exit__(self, *_) -> None:
        self._app.disconnect_and_stop()

    def _req_id(self) -> int:
        # The wrapper's get_next_valid_id repeats ids, which merges responses.
        req_id = self._next_req_id
        self._next_req_id += 1
        return req_id

    def _wait_for(self, key: str) -> object:
        # The wrapper's own waits miss replies that arrive before they start.
        deadline = time.time() + RESPONSE_WAIT_SECONDS
        while key not in self._app.response_data:
            if time.time() > deadline:
                raise RuntimeError(f"No response from TWS for {key}")
            time.sleep(0.05)
        return self._app.response_data.pop(key)

    def get_net_liquidation(self) -> float:
        req_id = self._req_id()
        self._app.reqAccountSummary(req_id, "All", AccountSummaryTags.NetLiquidation)
        summary = self._wait_for(f"account_summary_{req_id}")
        self._app.cancelAccountSummary(req_id)
        account = next(iter(summary))
        return float(summary[account]["NetLiquidation"]["value"])

    def get_positions(self) -> dy.DataFrame[Positions]:
        self._app.reqPositions()
        positions = self._wait_for("positions_0")
        self._app.cancelPositions()
        rows = [
            {
                "ticker": from_ibkr_symbol(position["contract"].symbol),
                "shares": float(position["position"]),
            }
            for account_positions in positions.values()
            for position in account_positions
            if position["contract"].secType == "STK"
            and float(position["position"]) != 0
        ]
        return Positions.validate(
            pl.DataFrame(rows, schema={"ticker": pl.String, "shares": pl.Float64})
        )

    def get_working_order_tickers(self) -> list[str]:
        """Tickers with open orders from TWS or any API client. Only reads them, and doesn't bind them."""
        self._app.open_orders = {}
        self._app.reqAllOpenOrders()
        self._wait_for("open_orders_0")
        return sorted(
            {
                from_ibkr_symbol(order["contract"].symbol)
                for order in self._app.open_orders.values()
            }
        )

    def _get_contract_details(self, tickers: list[str]) -> dict[str, dict]:
        req_ids = {}
        for ticker in tickers:
            req_id = self._req_id()
            req_ids[ticker] = req_id
            self._app.reqContractDetails(req_id, _stock_contract(ticker))
            time.sleep(REQUEST_PACING_SECONDS)

        deadline = time.time() + CONTRACT_WAIT_SECONDS
        pending = set(req_ids.values())
        while pending and time.time() < deadline:
            pending = {
                req_id
                for req_id in pending
                if f"contract_details_{req_id}" not in self._app.response_data
                and req_id not in self._app.errors
            }
            time.sleep(0.05)

        details = {}
        for ticker, req_id in req_ids.items():
            matches = self._app.contract_details.get(req_id, [])
            if matches:
                details[ticker] = {
                    "contract": matches[0].contract,
                    "min_tick": matches[0].minTick,
                    "primary_exchange": matches[0].contract.primaryExchange or None,
                }
        return details

    def _wait_for_two_sided_quotes(self, req_ids: list[int]) -> None:
        """Wait for bid and ask on every request. With the market closed this runs to the deadline."""

        def two_sided(req_id: int) -> bool:
            tick = self._app.market_data.get(req_id, {})
            return bool(
                _positive(tick.get("BID"), tick.get("DELAYED_BID"))
                and _positive(tick.get("ASK"), tick.get("DELAYED_ASK"))
            )

        deadline = time.time() + QUOTE_WAIT_SECONDS
        while time.time() < deadline and not all(map(two_sided, req_ids)):
            time.sleep(0.1)

    def get_quotes(self, tickers: list[str]) -> dy.DataFrame[Quotes]:
        """Bid, ask, last and close, live if subscribed and otherwise delayed."""
        details = self._get_contract_details(tickers)
        ticks: dict[str, dict] = {}

        known = [ticker for ticker in tickers if ticker in details]
        for start in range(0, len(known), MAX_MARKET_DATA_LINES):
            req_ids = {}
            for ticker in known[start : start + MAX_MARKET_DATA_LINES]:
                req_id = self._req_id()
                req_ids[ticker] = req_id
                self._app.reqMktData(
                    req_id, details[ticker]["contract"], "", False, False, []
                )
                time.sleep(REQUEST_PACING_SECONDS)

            self._wait_for_two_sided_quotes(list(req_ids.values()))

            for ticker, req_id in req_ids.items():
                self._app.cancelMktData(req_id)
                ticks[ticker] = self._app.market_data.get(req_id, {})

        rows = []
        for ticker in tickers:
            tick = ticks.get(ticker, {})
            detail = details.get(ticker, {})
            rows.append(
                {
                    "ticker": ticker,
                    "bid": _positive(tick.get("BID"), tick.get("DELAYED_BID")),
                    "ask": _positive(tick.get("ASK"), tick.get("DELAYED_ASK")),
                    "last": _positive(tick.get("LAST"), tick.get("DELAYED_LAST")),
                    "close": _positive(tick.get("CLOSE"), tick.get("DELAYED_CLOSE")),
                    "min_tick": detail.get("min_tick"),
                    "primary_exchange": detail.get("primary_exchange"),
                }
            )

        return Quotes.validate(pl.DataFrame(rows, schema=Quotes.to_polars_schema()))
