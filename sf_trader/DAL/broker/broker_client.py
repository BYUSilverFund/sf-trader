from abc import ABC, abstractmethod

from sf_trader.dal.models import PricesDF, OrdersDF, SharesDF


class BrokerClient(ABC):
    @abstractmethod
    def get_prices(self, tickers: list[str]) -> PricesDF:
        pass

    @abstractmethod
    def get_account_value(self) -> float:
        pass

    @abstractmethod
    def post_orders(self, orders: OrdersDF) -> None:
        pass

    @abstractmethod
    def get_positions(self) -> SharesDF:
        pass

    @abstractmethod
    def cancel_orders(self) -> None:
        pass
