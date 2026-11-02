import dataframely as dy


class Assets(dy.Schema):
    barrid = dy.String(nullable=False)
    ticker = dy.String(nullable=True)
    price = dy.Float64(nullable=True)
    in_universe = dy.Bool(nullable=True)
    predicted_beta = dy.Float64(nullable=True)
    specific_risk = dy.Float64(nullable=True)
    market_cap = dy.Float64(nullable=True)


class Positions(dy.Schema):
    ticker = dy.String(nullable=False)
    shares = dy.Float64(nullable=False)


class Quotes(dy.Schema):
    ticker = dy.String(nullable=False)
    bid = dy.Float64(nullable=True)
    ask = dy.Float64(nullable=True)
    last = dy.Float64(nullable=True)
    close = dy.Float64(nullable=True)
    min_tick = dy.Float64(nullable=True)
    primary_exchange = dy.String(nullable=True)


class Portfolio(dy.Schema):
    ticker = dy.String(nullable=False)
    barrid = dy.String(nullable=False)
    weight = dy.Float64(nullable=False)
    combined_alpha = dy.Float64(nullable=False)
    price = dy.Float64(nullable=False)
    shares = dy.Float64(nullable=False)


class Orders(dy.Schema):
    ticker = dy.String(nullable=False)
    action = dy.String(nullable=False)
    shares = dy.Float64(nullable=False)
    limit_price = dy.Float64(nullable=False)
    value = dy.Float64(nullable=False)
    primary_exchange = dy.String(nullable=True)
