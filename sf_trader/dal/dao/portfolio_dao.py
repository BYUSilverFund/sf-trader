from sf_trader.dal.models.db_model import Database
from sf_trader.dal.models.table_model import TableName
from sf_trader.dal.models.schema_models import WeightsDF, PricesDF, WeightsSchema, PricesSchema

import polars as pl
import datetime as dt


class PortfolioDAO(Database):
    """Data Access Object for portfolio-related operations."""

    def __init__(self):
        super().__init__()

    def get_optimal_weights_by_date(self, date: dt.date) -> WeightsDF:
        """Read optimal weights for a given date."""

        optimal_weights_table = self.get_table(TableName.OPTIMAL_WEIGHTS)
        weights = (
            optimal_weights_table.scan(year=date.year)
            .filter(pl.col('date').eq(date))
            .select('ticker', 'weight')
            .sort("ticker")
            .collect()
        )

        return WeightsSchema.validate(weights)

    def get_prices_by_date(self, date: dt.date, tickers: list[str]) -> PricesDF:
        """Read prices for a given date."""

        assets_table = self.get_table(TableName.ASSETS)
        prices = (
            assets_table.scan(year=date.year)
            .filter(pl.col('date').eq(date), pl.col("ticker").is_in(tickers))
            .select('ticker', 'price')
            .sort("ticker")
            .collect()
        )

        return PricesSchema.validate(prices)

    def get_universe_by_date(self, date: dt.date) -> list[str]:
        """Read universe tickers for a given date."""

        assets_table = self.get_table(TableName.ASSETS)
        tickers = (
            assets_table.scan(year=date.year)
            .filter(pl.col("date").eq(date), pl.col('in_universe'))
            .collect()
            .get_column("ticker")
            .unique()
            .sort("ticker")
            .to_list()
        )

        return tickers

    def get_benchmark_weights_by_date(self, date: dt.date) -> WeightsDF:
        """Read benchmark weights for a given date."""

        assets_table = self.get_table(TableName.ASSETS)
        weights = (
            assets_table.scan(year=date.year)
            .filter(pl.col("date").eq(date), pl.col('in_universe'))
            .select(
                "ticker",
                pl.col("market_cap")
                .truediv(pl.col("market_cap").sum())
                .over("date")
                .alias("weight"),
            )
            .sort("ticker")
            .collect()
        )

        return WeightsSchema.validate(weights)

    def get_ticker_barrid_mapping(self, date: dt.date) -> pl.DataFrame:
        assets_table = self.get_table(TableName.ASSETS)
        
        mapping = (assets_table.scan(
            year=date.year
        )
        .filter(
            pl.col("date").eq(date),
            pl.col('in_universe')
        )
        .collect()
        .select(["ticker", "barrid"])
        .unique()
        .sort("ticker")
        )

        return mapping