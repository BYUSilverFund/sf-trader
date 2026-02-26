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
