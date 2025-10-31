from sf_trader.broker.client import BrokerClient
import dataframely as dy
import polars as pl
from sf_trader.components.models import Prices, Orders, Shares
from ibapi.sync_wrapper import TWSSyncWrapper, Contract, Order
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

    @staticmethod
    def _convert_ticker_to_ibkr_format(ticker: str) -> str:
        """Convert ticker format from BRK.B to BRK B for IBKR API."""
        return ticker.replace(".", " ")

    @staticmethod
    def _convert_ticker_from_ibkr_format(ticker: str) -> str:
        """Convert ticker format from BRK B to BRK.B from IBKR API."""
        return ticker.replace(" ", ".")

    def get_prices(self, tickers: list[str]) -> dy.DataFrame[Prices]:
        prices_list = []

        for ticker in tqdm(tickers, desc="Fetching prices", disable=True):
            contract = Contract()
            contract.symbol = self._convert_ticker_to_ibkr_format(ticker)
            contract.secType = "STK"
            contract.exchange = "SMART"
            # contract.primaryExchange = "ISLAND"
            contract.currency = "USD"

            self._app.reqMarketDataType(3)  # TODO: Change to live data

            prices_raw: dict[str, dict[int, dict]] = self._app.get_market_data_snapshot(
                contract=contract, snapshot=False, timeout=5
            )

            prices_clean = {
                "ticker": ticker,
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
        for order_ in orders.to_dicts():
            try:
                contract = Contract()
                contract.symbol = self._convert_ticker_to_ibkr_format(order_.get("ticker"))
                contract.secType = "STK"
                contract.exchange = "SMART"
                contract.currency = "USD"

                order = Order()
                order.action = order_.get("action")
                order.orderType = "MKT"
                order.totalQuantity = order_.get("shares")

                self._app.place_order_sync(contract, order)

                print(f"✓ {order_.get('ticker')}: {order_.get('action')} {order_.get('shares')} @ MKT")
            except Exception as e:
                error_msg = str(e)
                if "No security definition" in error_msg or "200" in error_msg:
                    print(f"⚠ Skipping {order_.get('ticker')}: Security not found")
                else:
                    print(f"✗ Error placing order for {order_.get('ticker')}: {error_msg}")

            time.sleep(0.1)

    def get_positions(self) -> dy.DataFrame[Shares]:
        positions_summary: dict[str, list[dict]] = self._app.get_positions()
        client_account_id = list(positions_summary.keys())[0]
        positions_raw = positions_summary.get(client_account_id)

        positions_list = [
            {
                "ticker": self._convert_ticker_from_ibkr_format(position.get("contract").symbol),
                "shares": float(position.get("position")),
            }
            for position in positions_raw
        ]

        positions = pl.DataFrame(positions_list)

        return Shares.validate(positions)

    def cancel_orders(self) -> None:
        try:
            # Get all open orders
            open_orders = self._app.get_open_orders()

            if not open_orders:
                print("No open orders to cancel")
                return

            print(f"Found {len(open_orders)} open order(s)")

            # Cancel each order individually
            cancelled_count = 0
            for order_id, order_data in open_orders.items():
                try:
                    self._app.cancelOrder(order_id)
                    ticker = self._convert_ticker_from_ibkr_format(
                        order_data.get("contract").symbol
                    )
                    action = order_data.get("order").action
                    quantity = order_data.get("order").totalQuantity
                    print(f"✓ Cancelled order {order_id}: {ticker} {action} {quantity}")
                    cancelled_count += 1
                    time.sleep(0.1)
                except Exception as e:
                    print(f"✗ Error cancelling order {order_id}: {str(e)}")

            print(f"✓ Cancelled {cancelled_count}/{len(open_orders)} order(s)")

        except Exception as e:
            print(f"✗ Error getting open orders: {str(e)}")

    def __del__(self) -> None:
        self._app.disconnect_and_stop()


def ibrk_client() -> IBKRClient:
    return IBKRClient()
