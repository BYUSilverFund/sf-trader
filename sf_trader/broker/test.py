from sf_trader.broker.client import BrokerClient
import dataframely as dy
import polars as pl
import time

from sf_trader.components.models import Prices, Orders, Shares
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
        for order in orders.to_dicts():
            ticker = order['ticker']
            price = order['price']
            shares = order['shares']
            action = order['action']

            print(f"✓ {ticker}: {action} {shares} @ {price}")
            time.sleep(0.01)

    def get_shares(self) -> dy.DataFrame[Shares]:
        shares = pl.DataFrame(
            {
                "ticker": ["AAPL", "ACAD", "WRBY", "ZG"],
                "shares": [10000.0, 10000.0, 10000.0, 10000.0],
            }
        )

        return Shares.validate(shares)
