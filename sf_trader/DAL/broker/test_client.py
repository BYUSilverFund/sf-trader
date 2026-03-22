from sf_trader.dal.broker.broker_client import BrokerClient
import polars as pl
import time

from sf_trader.dal.models.schema_models import PricesDF, SharesDF, OrdersDF, PricesSchema, SharesSchema
import sf_quant.data as sfd
import datetime as dt


class TestClient(BrokerClient):
    def __init__(self, data_date: dt.date) -> None:
        self._data_date = data_date

    def get_prices(self, tickers: list[str]) -> PricesDF:
        prices = sfd.load_assets_by_date(
            date_=self._data_date, columns=["ticker", "price"], in_universe=True
        ).sort("ticker", "price")

        return PricesSchema.validate(prices)

    def get_account_value(self) -> float:
        return float(1e6)

    def post_orders(self, orders: OrdersDF) -> None:
        for order in orders.to_dicts():
            ticker = order["ticker"]
            price = order["price"]
            shares = order["shares"]
            action = order["action"]

            print(f"✓ {ticker}: {action} {shares} @ {price}")
            time.sleep(0.01)

    def get_positions(self) -> SharesDF:
        shares = pl.DataFrame(
            {
                "ticker": ["AAPL", "ACAD", "WRBY", "ZG"],
                "shares": [10000.0, 10000.0, 10000.0, 10000.0],
            }
        )

        return SharesSchema.validate(shares)
    
    def cancel_orders(self) -> None:
        return None