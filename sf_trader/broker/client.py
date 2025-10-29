from abc import ABC, abstractmethod

import dataframely as dy

from sf_trader.components.models import Prices, Orders, Shares


class BrokerClient(ABC):
    @abstractmethod
    def get_prices(self, tickers: list[str]) -> dy.DataFrame[Prices]:
        pass

    @abstractmethod
    def get_account_value(self) -> float:
        pass

    @abstractmethod
    def post_orders(self, orders: dy.DataFrame[Orders]) -> None:
        pass

    @abstractmethod
    def get_positions(self) -> dy.DataFrame[Shares]:
        pass
