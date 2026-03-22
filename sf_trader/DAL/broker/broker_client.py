from abc import ABC, abstractmethod

<<<<<<< HEAD:sf_trader/DAL/broker/broker_client.py
from sf_trader.dal.models import PricesDF, OrdersDF, SharesDF
=======
import dataframely as dy

from components.models import Prices, Orders, Shares
>>>>>>> origin/main:sf_trader/broker/client.py


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
