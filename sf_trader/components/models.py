from dataclasses import dataclass
from typing import Callable, List

import dataframely as dy
import polars as pl


@dataclass
class Signal:
    name: str
    expr: pl.Expr
    lookback_days: int


@dataclass
class SignalCombinator:
    """Combines multiple signals into a single alpha"""

    name: str
    combine_fn: Callable[[List[str]], pl.Expr]

    def apply(self, signals: List[Signal]) -> pl.Expr:
        """
        Apply the combinator to a list of signals

        Args:
            signals: List of Signal objects

        Returns:
            Polars expression that combines the signals
        """
        signal_names = [signal.name for signal in signals]
        return self.combine_fn(signal_names)


class Assets(dy.Schema):
    date = dy.Date(nullable=False)
    barrid = dy.String(nullable=False)
    ticker = dy.String(nullable=False)
    return_ = dy.Float64(nullable=False, alias="return")
    predicted_beta = dy.Float64(nullable=True)
    specific_risk = dy.Float64(nullable=True)


class Prices(dy.Schema):
    ticker = dy.String(nullable=False)
    price = dy.Float64(nullable=False)


class Dollars(dy.Schema):
    ticker = dy.String(nullable=False)
    dollars = dy.Float64(nullable=False)


class Shares(dy.Schema):
    ticker = dy.String(nullable=False)
    shares = dy.Float64(nullable=False)


class Weights(dy.Schema):
    ticker = dy.String(nullable=False)
    weight = dy.Float64(nullable=False)


class Alphas(dy.Schema):
    ticker = dy.String(nullable=False)
    alpha = dy.Float64(nullable=False)


class Betas(dy.Schema):
    ticker = dy.String(nullable=False)
    predicted_beta = dy.Float64(nullable=False)


class Orders(dy.Schema):
    ticker = dy.String(nullable=False)
    price = dy.Float64(nullable=False)
    shares = dy.Float64(nullable=False)
    action = dy.String(nullable=False)


@dataclass
class PortfolioMetrics:
    gross_exposure: float
    net_exposure: float
    num_long: int
    num_short: int
    num_positions: int
    active_risk: float
    total_risk: float
    utilization: float
    account_value: float
    dollars_allocated: float
