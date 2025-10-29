from sf_trader.broker.client import BrokerClient
import dataframely as dy

from sf_trader.components.models import Prices, Orders
import sf_quant.data as sfd
from sf_trader.config import Config


class TestClient(BrokerClient):
    def __init__(self, config: Config) -> None:
        self._config = config

    def get_prices(self, tickers: list[str]) -> dy.DataFrame[Prices]:
        prices = sfd.load_assets_by_date(
            date_=self._config.data_date, columns=["ticker", "price"], in_universe=True
        ).sort("ticker", "price")

        return Prices.validate(prices)

    def get_account_value(self) -> float:
        return float(1e6)

    def post_orders(self, orders: dy.DataFrame[Orders]) -> None:
        pass
