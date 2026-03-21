from sf_trader.config import Config

from sf_trader.dal.dao.portfolio_dao import PortfolioDAO
from sf_trader.dal.dao.surface_dao import SurfaceDAO
from sf_trader.dal.models.schema_models import PricesDF, SharesDF, OrdersDF, OrdersSchema

import polars as pl


class OrderService:
    def __init__(
        self, 
        config: Config, 
        portfolio_dao: PortfolioDAO | None = None, 
        surface_dao: SurfaceDAO | None = None
    ):
        self.portfolio_dao = portfolio_dao or PortfolioDAO()
        self.surface_dao = surface_dao or SurfaceDAO(config)
        self.config = config
        self.broker = config.broker
    

    def get_write_orders(self) -> OrdersDF:
        """Reads optimal shares and computes orders, then writes orders to surface"""

        # Get optimal shares from surface
        optimal_shares = self.surface_dao.read_portfolio()

        # Get current shares
        current_shares = self.broker.get_positions()

        # Compute ticker list
        tickers = list(
            set(current_shares["ticker"].to_list() + optimal_shares["ticker"].to_list())
        )

        # Get live prices
        # TODO: Change to live price?
        prices = self.portfolio_dao.get_prices_by_date(date=self.config.data_date, tickers=tickers)

        # Get order deltas
        orders = self.get_order_deltas(
            current_shares=current_shares, optimal_shares=optimal_shares, prices=prices
        )

        # Write orders to surface
        self.surface_dao.write_orders(OrdersSchema.validate(orders))

        return orders


    def post_orders(self) -> None:
        # Connect to broker
        broker = self.broker

        # Get orders from surface
        orders = self.surface_dao.read_orders()

        # Execute trades
        broker.post_orders(orders=orders)


    def cancel_orders(self) -> None:
        # Connect to broker
        broker = self.broker

        # Cancel all open orders
        broker.cancel_orders()


    def get_order_deltas(
        self,
        prices: PricesDF,
        current_shares: SharesDF,
        optimal_shares: SharesDF,
    ) -> OrdersDF:
        # Prep shares dataframes for join
        current_shares = current_shares.rename({"shares": "current_shares"})
        optimal_shares = optimal_shares.rename({"shares": "optimal_shares"})

        orders = (
            prices
            # Joins
            .join(current_shares, on="ticker", how="left")
            .join(optimal_shares, on="ticker", how="left")
            # Fill nulls with 0
            .with_columns(pl.col("current_shares", "optimal_shares").fill_null(0))
            # Compute share differential
            .with_columns(pl.col("optimal_shares").sub("current_shares").alias("shares"))
            # Compute order side
            .with_columns(
                pl.when(pl.col("shares").gt(0))
                .then(pl.lit("BUY"))
                .when(pl.col("shares").lt(0))
                .then(pl.lit("SELL"))
                .otherwise(pl.lit("HOLD"))
                .alias("action")
            )
            # Absolute value the shares
            .with_columns(pl.col("shares").abs())
            # Select
            .select("ticker", "price", "shares", "action")
            # Filter
            .filter(
                pl.col("ticker")
                .is_in(self.config.ignore_tickers)
                .not_(),  # Ignore problematic tickers
                pl.col("shares").ne(0),  # Remove 0 share trades
                pl.col("action").ne("HOLD"),  # Remove HOLDs
                pl.col("price").is_not_null(),  # Remove unknown prices
            )
            # Sort
            .sort("ticker")
        )

        return OrdersSchema.validate(orders)
