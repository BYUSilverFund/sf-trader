from sf_trader.broker.client import BrokerClient
import dataframely as dy
import polars as pl
from sf_trader.components.models import Prices, Orders, Shares
from ibapi.sync_wrapper import TWSSyncWrapper, Contract
from ibapi.account_summary_tags import AccountSummaryTags
from rich import print
from tqdm import tqdm
import time


class IBKRClient(BrokerClient):
    def __init__(self) -> None:
        self._app = TWSSyncWrapper(timeout=30)
        if not self._app.connect_and_start(
            host="127.0.0.1", port=7497, client_id=8675309
        ):
            raise RuntimeError("Failed to connect to TWS!")
        else:
            print("Connected to TWS")

    def get_prices(self, tickers: list[str]) -> dy.DataFrame[Prices]:
        prices_list = []

        for ticker in tqdm(tickers, desc="Fetching prices", disable=True):
            contract = Contract()
            contract.symbol = ticker
            contract.secType = "STK"
            contract.exchange = "SMART"
            contract.primaryExchange = "ISLAND"
            contract.currency = "USD"

            self._app.reqMarketDataType(3)  # TODO: Change to live data

            prices_raw: dict[str, dict[int, dict]] = self._app.get_market_data_snapshot(
                contract=contract, snapshot=False, timeout=5
            )

            prices_clean = {
                "id": ticker,
                "bid": prices_raw.get("price").get(66).get("price"),
                "ask": prices_raw.get("price").get(67).get("price"),
                "last": prices_raw.get("price").get(68).get("price"),
                "bid_size": float(prices_raw.get("size").get(69)),
                "ask_size": float(prices_raw.get("size").get(70)),
                "last_size": float(prices_raw.get("size").get(71)),
            }

            prices_list.append(prices_clean)

            time.sleep(1)

        prices = pl.DataFrame(prices_list)

        return prices

    def get_account_value(self) -> float:
        account_summary: dict[str, dict[str, dict[str, str]]] = (
            self._app.get_account_summary(AccountSummaryTags.NetLiquidation, timeout=5)
        )
        client_account_id = list(account_summary.keys())[0]
        net_liquidation_value = (
            account_summary.get(client_account_id).get("NetLiquidation").get("value")
        )
        return float(net_liquidation_value)

    def post_orders(self, orders: dy.DataFrame[Orders]) -> None:
        pass

    def get_positions(self) -> dy.DataFrame[Shares]:
        positions_summary: dict[str, list[dict]] = self._app.get_positions()
        client_account_id = list(positions_summary.keys())[0]
        positions_raw = positions_summary.get(client_account_id)

        positions_list = [
            {
                "id": position.get("contract").symbol,
                "shares": float(position.get("position")),
            }
            for position in positions_raw
        ]

        positions = pl.DataFrame(positions_list)

        return Shares.validate(positions)

    def __del__(self) -> None:
        self._app.disconnect_and_stop()


def ibrk_client() -> IBKRClient:
    return IBKRClient()
