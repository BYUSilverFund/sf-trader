import polars as pl
import os

from sf_trader.config import Config
from sf_trader.dal.models.schema_models import SharesDF, OrdersDF, OrdersSchema, SharesSchema


class SurfaceDAO:
    def __init__(self, config: Config):
        self.config = config
    

    def write_orders(self, orders: OrdersDF) -> None:
        
        path_ = self.config.orders_path
        orders.write_csv(path_)


    def read_orders(self) -> OrdersDF:
        
        path_ = self.config.orders_path

        if not os.path.exists(path_):
                raise FileNotFoundError(f"Orders file not found at path: {path_}")

        return OrdersSchema.validate(pl.read_csv(path_))
    

    def write_portfolio(self, shares: SharesDF) -> None:

        path_ = self.config.portfolio_path
        shares.write_csv(path_)


    def read_portfolio(self) -> SharesDF:
        
        path_ = self.config.portfolio_path
        if not os.path.exists(path_):
                raise FileNotFoundError(f"Portfolio file not found at path: {path_}")

        return SharesSchema.validate(pl.read_csv(path_))