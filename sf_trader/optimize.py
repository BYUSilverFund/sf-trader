from dataclasses import dataclass

import cvxpy as cp
import dataframely as dy
import numpy as np
import polars as pl
import sf_quant.optimizer as sfo

from sf_trader.config import Constraints
from sf_trader.models import Portfolio


class OptimizationError(Exception):
    """Raised when the optimizer cannot produce weights"""


@dataclass(frozen=True)
class MaxWeight:
    limit: float

    def __call__(self, weights: cp.Variable, **kwargs) -> cp.Constraint:
        return weights <= self.limit


def build_constraints(constraints: Constraints) -> list[sfo.constraints.Constraint]:
    flags = {
        "long_only": sfo.LongOnly,
        "full_investment": sfo.FullInvestment,
        "no_buying_on_margin": sfo.NoBuyingOnMargin,
        "unit_beta": sfo.UnitBeta,
        "zero_beta": sfo.ZeroBeta,
        "zero_investment": sfo.ZeroInvestment,
    }
    built = [
        constraint() for name, constraint in flags.items() if getattr(constraints, name)
    ]
    if constraints.max_weight is not None:
        built.append(MaxWeight(constraints.max_weight))
    return built


def clean_weights(weights: pl.DataFrame, constraints: Constraints) -> pl.DataFrame:
    """Drop dust and round. The solver only meets the budget approximately, so restore it first."""
    decimals = constraints.weight_decimals
    kept = weights.filter(pl.col("weight").abs().ge(10**-decimals))
    if constraints.full_investment:
        kept = kept.with_columns(pl.col("weight").truediv(pl.col("weight").sum()))
    return kept.with_columns(pl.col("weight").round(decimals))


def optimize_portfolio(
    alphas: pl.DataFrame,
    covariance_matrix: np.ndarray,
    constraints: Constraints,
    capital: float,
) -> dy.DataFrame[Portfolio]:
    """Solve for weights. alphas must be sorted by barrid to match covariance_matrix."""
    try:
        weights = sfo.mve_optimizer(
            ids=alphas["barrid"].to_list(),
            alphas=alphas["combined_alpha"].to_numpy(),
            betas=alphas["predicted_beta"].to_numpy(),
            covariance_matrix=covariance_matrix,
            constraints=build_constraints(constraints),
            gamma=constraints.risk_aversion,
        )
    except (cp.error.SolverError, ValueError, TypeError) as e:
        raise OptimizationError(f"Optimizer failed: {e}") from e

    if weights["weight"].is_null().any() or weights["weight"].is_nan().any():
        raise OptimizationError("Optimizer returned no solution; check the constraints")

    portfolio = (
        clean_weights(weights, constraints)
        .join(alphas, on="barrid", how="left")
        .with_columns(
            pl.col("weight").mul(capital).truediv(pl.col("price")).round(4).alias("shares")
        )
        .select("ticker", "barrid", "weight", "combined_alpha", "price", "shares")
        .sort("ticker")
    )

    return Portfolio.validate(portfolio)
