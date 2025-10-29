from sf_trader.broker.client import BrokerClient
import dataframely as dy

from sf_trader.components.models import Prices, Orders
from sf_trader.config import Config


class IBKRClient(BrokerClient):
    def __init__(self, config: Config) -> None:
        self._config = config

    def get_prices(self, tickers: list[str]) -> dy.DataFrame[Prices]:
        pass

    def get_account_value(self) -> float:
        return float(1e6)

    def post_orders(self, orders: dy.DataFrame[Orders]) -> None:
        pass
