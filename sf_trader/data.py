import datetime as dt

import dataframely as dy
import numpy as np
import polars as pl
import sf_quant.data as sfd
from sf_quant.data._tables import optimal_weights_table

from sf_trader.models import Assets

MAX_WEIGHTS_AGE_DAYS = 4
WEIGHT_DECIMALS = 4
MIN_WEIGHT = 10**-WEIGHT_DECIMALS


class DataError(Exception):
    """Raised when required data is missing or stale"""


def _scan_weights(year: int) -> pl.LazyFrame | None:
    try:
        lazy = optimal_weights_table.scan(year)
        lazy.collect_schema()
    except FileNotFoundError:
        return None
    return lazy


def resolve_weights_date(trade_date: dt.date) -> dt.date:
    """Weights dated D are traded on the next session, so use the latest date before trade_date."""
    dates = []
    for year in (trade_date.year - 1, trade_date.year):
        lazy = _scan_weights(year)
        if lazy is not None:
            dates.append(
                lazy.filter(pl.col("date").lt(trade_date))
                .select(pl.col("date").max())
                .collect()
                .item()
            )

    weights_date = max((date for date in dates if date is not None), default=None)
    if weights_date is None:
        raise DataError(f"No optimal weights found before {trade_date}")

    age = (trade_date - weights_date).days
    if age > MAX_WEIGHTS_AGE_DAYS:
        raise DataError(
            f"Latest optimal weights are from {weights_date}, {age} days before {trade_date}. "
            f"Refresh the data (limit is {MAX_WEIGHTS_AGE_DAYS} days)."
        )

    return weights_date


def load_weights(weights_date: dt.date) -> pl.DataFrame:
    """Holdings from the optimal_weights table. Most rows are solver noise near 0."""
    weights = (
        _scan_weights(weights_date.year)
        .filter(pl.col("date").eq(weights_date))
        .select("barrid", "weight")
        .collect()
    )

    total = weights["weight"].sum()
    missing = weights["weight"].is_null().any() or weights["weight"].is_nan().any()
    if missing or abs(total - 1) > 1e-4 or weights["weight"].min() < -MIN_WEIGHT:
        raise DataError(
            f"Optimal weights for {weights_date} are invalid (sum {total:.4f}). "
            "Check the nightly optimal_weights job before trading."
        )

    return weights.filter(pl.col("weight").ge(MIN_WEIGHT)).sort("barrid")


def load_assets(date: dt.date) -> dy.DataFrame[Assets]:
    assets = sfd.load_assets_by_date(
        date_=date,
        in_universe=False,
        columns=[
            "barrid",
            "ticker",
            "price",
            "in_universe",
            "predicted_beta",
            "specific_risk",
            "market_cap",
        ],
    )
    if assets.is_empty():
        raise DataError(f"No assets on {date}")

    return Assets.validate(assets)


def load_exposure_barrids(date: dt.date) -> list[str]:
    return sfd.load_exposures_by_date(date)["barrid"].to_list()


def load_covariance_matrix(date: dt.date, barrids: list[str]) -> np.ndarray:
    """Covariance in decimal space, with rows and columns in the order of sorted barrids."""
    return (
        sfd.construct_covariance_matrix(date_=date, barrids=sorted(barrids))
        .drop("barrid")
        .to_numpy()
    )
