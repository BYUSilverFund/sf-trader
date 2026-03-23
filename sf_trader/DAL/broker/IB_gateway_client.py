from sf_trader.dal.broker.broker_client import BrokerClient
from sf_trader.dal.models.schema_models import PricesDF, OrdersDF, SharesDF
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

        if connect:
            if not self._app.connect_and_start(
                host=host,
                port=port,
                client_id=client_id,
            ):
                raise RuntimeError("Failed to connect to IB Gateway")

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
        raise NotImplementedError("Implement account summary retrieval for IB Gateway")

    def post_orders(self, orders: OrdersDF) -> None:
        raise NotImplementedError("Implement order placement for IB Gateway")

    def get_positions(self) -> SharesDF:
        raise NotImplementedError("Implement position retrieval for IB Gateway")

    def cancel_orders(self) -> None:
        raise NotImplementedError("Implement open order cancellation for IB Gateway")

    def disconnect(self) -> None:
        if hasattr(self, "_app") and self._app is not None:
            self._app.disconnect_and_stop()
            print("Disconnected from IB Gateway")

    def __del__(self) -> None:
        try:
            self.disconnect()
        except Exception:
            pass