import polars as pl
import time

from sf_trader.dal.broker.broker_client import BrokerClient
from sf_trader.dal.models.schema_models import PricesDF, OrdersDF, SharesDF, SharesSchema
from ibapi.sync_wrapper import TWSSyncWrapper, Contract, Order, OrderCancel
from ibapi.account_summary_tags import AccountSummaryTags
from rich import print


class IBGatewayClient(BrokerClient):
    def __init__(
        self,
        app: TWSSyncWrapper | None = None,
        host: str = "127.0.0.1",
        port: int = 4002,
        client_id: int = 8675309,
        timeout: int = 30,
        connect: bool = True,
    ) -> None:
        self._app = app or TWSSyncWrapper(timeout=timeout)
        self._install_ib_message_filter()

        if connect:
            if not self._app.connect_and_start(
                host=host,
                port=port,
                client_id=client_id,
            ):
                raise RuntimeError("Failed to connect to IB Gateway")
            
        print("Connected to IB Gateway")

    def _install_ib_message_filter(self) -> None:
        original_error = self._app.error

        info_codes = {2104, 2106, 2107, 2108, 2158}
        warning_codes = {2103, 2105, 2110, 1100, 1101, 1102}

        def filtered_error(*args):
            advanced_json = ""

            if len(args) == 4:
                req_id, error_code, error_string, advanced_json = args
                error_time = None
            elif len(args) == 5:
                req_id, error_time, error_code, error_string, advanced_json = args
            else:
                return original_error(*args)

            if error_code in info_codes:
                print(f"INFO {req_id} {error_code} {error_string}")
                return

            if error_code in warning_codes:
                print(f"WARN {req_id} {error_code} {error_string}")
                return

            return original_error(*args)

        self._app.error = filtered_error
    
    @staticmethod
    def _convert_ticker_to_ibkr_format(ticker: str) -> str:
        """Convert ticker format from BRK.B to BRK B for IBKR API."""
        return ticker.replace(".", " ")

    @staticmethod
    def _convert_ticker_from_ibkr_format(ticker: str) -> str:
        """Convert ticker format from BRK B to BRK.B from IBKR API."""
        return ticker.replace(" ", ".")

    def _build_stock_contract(self, ticker: str) -> Contract:
        contract = Contract()
        contract.symbol = self._convert_ticker_to_ibkr_format(ticker)
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        return contract

    def get_prices(self, tickers: list[str]) -> PricesDF:
        raise NotImplementedError("Implement market data retrieval for IB Gateway")

    def get_account_value(self) -> float:
        account_summary: dict[str, dict[str, dict[str, str]]] = (
            self._app.get_account_summary(AccountSummaryTags.NetLiquidation, timeout=5)
        )
        client_account_id = list(account_summary.keys())[0]
        net_liquidation_value = (
            account_summary.get(client_account_id).get("NetLiquidation").get("value")
        )
        return float(net_liquidation_value)

    def post_orders(self, orders: OrdersDF) -> None:
        for order_ in orders.to_dicts():
            try:
                contract = Contract()
                contract.symbol = self._convert_ticker_to_ibkr_format(
                    order_.get("ticker")
                )
                contract.secType = "STK"
                contract.exchange = "SMART"
                contract.currency = "USD"

                order = Order()
                order.action = order_.get("action")
                order.orderType = "MKT"
                order.totalQuantity = order_.get("shares")
                order.tif = 'DAY'

                self._app.place_order_sync(contract, order)

                print(
                    f"✓ {order_.get('ticker')}: {order_.get('action')} {order_.get('shares')} @ MKT"
                )
            except Exception as e:
                error_msg = str(e)
                if "No security definition" in error_msg or "200" in error_msg:
                    print(f"⚠ Skipping {order_.get('ticker')}: Security not found")
                else:
                    print(
                        f"✗ Error placing order for {order_.get('ticker')}: {error_msg}"
                    )

            time.sleep(0.1)

    def get_positions(self) -> SharesDF:
        positions_summary: dict[str, list[dict]] = self._app.get_positions()
        client_account_id = list(positions_summary.keys())[0]
        positions_raw = positions_summary.get(client_account_id)

        positions_list = [
            {
                "ticker": self._convert_ticker_from_ibkr_format(
                    position.get("contract").symbol
                ),
                "shares": float(position.get("position")),
            }
            for position in positions_raw
        ]

        positions = pl.DataFrame(positions_list)

        return SharesSchema.validate(positions)

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
                    order_cancel = OrderCancel()
                    self._app.cancel_order_sync(order_id, orderCancel=order_cancel)
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

    def disconnect(self) -> None:
        if hasattr(self, "_app") and self._app is not None:
            self._app.disconnect_and_stop()
            print("Disconnected from IB Gateway")

    def __del__(self) -> None:
        try:
            self.disconnect()
        except Exception:
            pass