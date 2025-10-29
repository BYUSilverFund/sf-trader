from sf_trader.broker.client import BrokerClient
import dataframely as dy

from sf_trader.components.models import Prices, Orders


class IBKRClient(BrokerClient):
    def get_prices(self, tickers: list[str]) -> dy.DataFrame[Prices]:
        pass

    def get_account_value(self) -> float:
        return float(1e6)

    def post_orders(self, orders: dy.DataFrame[Orders]) -> None:
        pass
