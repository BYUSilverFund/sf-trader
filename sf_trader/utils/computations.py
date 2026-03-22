import polars as pl
from sf_trader.config import Config
from sf_trader.utils.tables_ui import PortfolioMetrics

import numpy as np

from sf_trader.dal.models.schema_models import (
    DollarsDF,
    SharesDF,
    WeightsDF,
    PricesDF,
)

_config = None


def set_config(config: Config) -> None:
    global _config
    _config = config



def compute_risk(weights: np.ndarray, covariance_matrix: np.ndarray) -> float:
    return np.sqrt(weights @ covariance_matrix @ weights.T)








def decompose_weights(
    benchmark: WeightsDF, weights: WeightsDF
) -> tuple[np.ndarray]:
    decomposed_weights = (
        benchmark.select("ticker", pl.col("weight").alias("weight_bmk"))
        .join(
            weights,
            on="ticker",
            how="left",
        )
        .with_columns(
            pl.col("weight").fill_null(0),
        )
        .with_columns(pl.col("weight").sub(pl.col("weight_bmk")).alias("weight_act"))
        .sort("ticker")
    )

    total_weights = decomposed_weights["weight"].to_numpy()
    active_weights = decomposed_weights["weight_act"].to_numpy()

    return total_weights, active_weights


def get_portfolio_metrics(
    total_weights: np.ndarray,
    active_weights: np.ndarray,
    covariance_matrix: np.ndarray,
    account_value: float,
    dollars_allocated: float,
) -> PortfolioMetrics:
    # Compute metrics
    gross_exposure = np.sum(np.abs(total_weights))
    net_exposure = np.sum(total_weights)
    num_long = int(np.sum(total_weights > 0))
    num_short = int(np.sum(total_weights < 0))
    num_positions = num_long + num_short
    active_risk = compute_risk(active_weights, covariance_matrix)
    total_risk = compute_risk(total_weights, covariance_matrix)
    utilization = dollars_allocated / account_value

    return PortfolioMetrics(
        gross_exposure=gross_exposure,
        net_exposure=net_exposure,
        num_long=num_long,
        num_short=num_short,
        num_positions=num_positions,
        active_risk=active_risk,
        total_risk=total_risk,
        utilization=utilization,
        account_value=account_value,
        dollars_allocated=dollars_allocated,
    )


def get_top_long_positions(
    shares: SharesDF,
    prices: PricesDF,
    dollars: DollarsDF,
    weights: WeightsDF,
    benchmark: WeightsDF,
    account_value: float,
    top_n: int = 10,
) -> pl.DataFrame:
    # Join all data together
    positions = (
        shares.join(prices, on="ticker", how="left")
        .join(dollars, on="ticker", how="left")
        .join(weights, on="ticker", how="left")
        .join(
            benchmark.select("ticker", pl.col("weight").alias("weight_bmk")),
            on="ticker",
            how="left",
        )
        .with_columns(
            pl.col("weight_bmk").fill_null(0),
        )
        .with_columns(pl.col("weight").sub(pl.col("weight_bmk")).alias("weight_act"))
        .with_columns(
            pl.when(pl.col("weight_bmk") != 0)
            .then((pl.col("weight_act") / pl.col("weight_bmk")) * 100)
            .otherwise(None)
            .alias("pct_chg_bmk")
        )
        .filter(pl.col("dollars") > 0)  # Only long positions
        .sort("dollars", descending=True)
        .head(top_n)
        .select(
            "ticker",
            "shares",
            "price",
            "dollars",
            "weight",
            "weight_bmk",
            "weight_act",
            "pct_chg_bmk",
        )
    )

    return positions
