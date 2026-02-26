import polars as pl

from sf_trader.config import Config
from sf_trader.dal.models.schema_models import SharesDF, OrdersDF, OrdersSchema, SharesSchema


class SurfaceDAO:
    def __init__(self, config: Config):
        self.config = config
    

    def write_orders(self, orders: OrdersDF) -> None:

        path = self.config.orders_path
        orders.write_csv(path)


    def read_orders(self) -> OrdersDF:
        
        path = self.config.orders_path
        return OrdersSchema.validate(pl.read_csv(path))
    

    def write_portfolio(self, shares: SharesDF) -> None:

        path = self.config.portfolio_path
        shares.write_csv(path)


    def read_portfolio(self) -> SharesDF:
        
        path = self.config.portfolio_path
        return SharesSchema.validate(pl.read_csv(path))