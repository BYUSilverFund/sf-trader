from sf_trader.dal.models.db_model import Database
from sf_trader.dal.models.table_model import TableName
from sf_trader.dal.models.schema_models import WeightsDF, WeightsSchema

import polars as pl

import datetime as dt

class PortfolioDAO(Database):
    """Data Access Object for portfolio-related operations."""

    def __init__(self):
        super().__init__()


    def get_optimal_weights_by_date(self, date: dt.date) -> WeightsDF:
        """Read optimal weights for a given date."""

        optimal_weights_table = self.get_table(TableName.OPTIMAL_WEIGHTS)
        weights = (optimal_weights_table.scan(
            year=date.year
        )
        .filter(
            pl.col('date').eq(date)
        )
        .select('ticker', 'weight')
        .collect()
        )

        return WeightsSchema.validate(weights)