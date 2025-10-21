from dataclasses import dataclass
import polars as pl
from typing import Callable, List
from sf_quant.optimizer.constraints import Constraint

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
class Config:
    """Trading system configuration"""
    # Universe
    ignore_tickers: list[str]
    
    # Signals
    signals: list[Signal]
    ic: float
    signal_combinator: SignalCombinator
    
    # Portfolio optimization
    constraints: list[Constraint]
    gamma: float
    decimal_places: int
