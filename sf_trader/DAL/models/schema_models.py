import dataframely as dy
from typing import TypeAlias


class WeightsSchema(dy.Schema):
    ticker = dy.String(nullable=False)
    weight = dy.Float64(nullable=False)

class PricesSchema(dy.Schema):
    ticker = dy.String(nullable=False)
    price = dy.Float64(nullable=False)

WeightsDF: TypeAlias = dy.DataFrame[WeightsSchema]
PricesDF: TypeAlias = dy.DataFrame[PricesSchema]