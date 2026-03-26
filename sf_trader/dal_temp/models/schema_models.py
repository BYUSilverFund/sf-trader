import dataframely as dy
from typing import TypeAlias


class AssetsSchema(dy.Schema):
    date = dy.Date(nullable=False)
    barrid = dy.String(nullable=False)
    ticker = dy.String(nullable=False)
    return_ = dy.Float64(nullable=False, alias="return")
    predicted_beta = dy.Float64(nullable=True)
    specific_risk = dy.Float64(nullable=True)

class PricesSchema(dy.Schema):
    ticker = dy.String(nullable=False)
    price = dy.Float64(nullable=False)

class DollarsSchema(dy.Schema):
    ticker = dy.String(nullable=False)
    dollars = dy.Float64(nullable=False)

class SharesSchema(dy.Schema):
    ticker = dy.String(nullable=False)
    shares = dy.Float64(nullable=False)

class WeightsSchema(dy.Schema):
    ticker = dy.String(nullable=False)
    weight = dy.Float64(nullable=False)

class AlphasSchema(dy.Schema):
    ticker = dy.String(nullable=False)
    alpha = dy.Float64(nullable=False)

class BetasSchema(dy.Schema):
    ticker = dy.String(nullable=False)
    predicted_beta = dy.Float64(nullable=False)

class OrdersSchema(dy.Schema):
    ticker = dy.String(nullable=False)
    price = dy.Float64(nullable=False)
    shares = dy.Float64(nullable=False)
    action = dy.String(nullable=False)


AssetsDF: TypeAlias = dy.DataFrame[AssetsSchema]
PricesDF: TypeAlias = dy.DataFrame[PricesSchema]
DollarsDF: TypeAlias = dy.DataFrame[DollarsSchema]
SharesDF: TypeAlias = dy.DataFrame[SharesSchema]
WeightsDF: TypeAlias = dy.DataFrame[WeightsSchema]
AlphasDF: TypeAlias = dy.DataFrame[AlphasSchema]
BetasDF: TypeAlias = dy.DataFrame[BetasSchema]
OrdersDF: TypeAlias = dy.DataFrame[OrdersSchema]