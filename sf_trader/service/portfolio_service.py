from sf_trader.config import Config

from sf_trader.dal.dao.portfolio_dao import PortfolioDAO
from sf_trader.dal.dao.surface_dao import SurfaceDAO
from sf_trader.dal.models.schema_models import SharesDF, SharesSchema, WeightsDF, PricesDF

import polars as pl


class PortfolioService:
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


    @staticmethod
    def get_optimal_shares(
        weights: WeightsDF, prices: PricesDF, account_value: float
    ) -> SharesDF:
        optimal_shares = (
            weights.join(prices, on="ticker", how="left")
            .with_columns(pl.lit(account_value).mul(pl.col("weight")).alias("dollars"))
            .with_columns(
                pl.col("dollars").truediv(pl.col("price")).floor().alias("shares")
            )
            .select(
                "ticker",
                "shares",
            )
        )

        return SharesSchema.validate(optimal_shares)
    

    def get_portfolio(self, config: Config) -> SharesDF:

        # Get universe
        universe = self.portfolio_dao.get_universe_by_date(date=config.data_date)

        # Get account value
        account_value = self.broker.get_account_value()

        # Get prices
        prices = self.portfolio_dao.get_prices_by_date(date=config.data_date, tickers=universe)

        # Get optimal weights
        optimal_weights = self.portfolio_dao.get_optimal_weights_by_date(date=config.data_date)

        # Get optimal shares
        optimal_shares = self.get_optimal_shares(
            weights=optimal_weights, prices=prices, account_value=account_value
        )

        return SharesSchema.validate(optimal_shares)