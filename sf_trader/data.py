import datetime as dt

import dataframely as dy
import numpy as np
import polars as pl
import sf_quant.data as sfd
from sf_quant.data._tables import alphas_table

from sf_trader.models import Assets

MAX_ALPHA_AGE_DAYS = 4


class DataError(Exception):
    """Raised when required data is missing or stale"""


def _scan_alphas(year: int) -> pl.LazyFrame | None:
    # Yearly files have different schemas, so never scan across all of them.
    try:
        lazy = alphas_table.scan(year)
        lazy.collect_schema()
    except FileNotFoundError:
        return None
    return lazy


def resolve_alpha_date(trade_date: dt.date) -> dt.date:
    """Alphas dated D are traded on the next session, so use the latest date before trade_date."""
    dates = []
    for year in (trade_date.year - 1, trade_date.year):
        lazy = _scan_alphas(year)
        if lazy is not None:
            dates.append(
                lazy.filter(pl.col("date").lt(trade_date))
                .select(pl.col("date").max())
                .collect()
                .item()
            )

    alpha_date = max((date for date in dates if date is not None), default=None)
    if alpha_date is None:
        raise DataError(f"No alphas found before {trade_date}")

    age = (trade_date - alpha_date).days
    if age > MAX_ALPHA_AGE_DAYS:
        raise DataError(
            f"Latest alphas are from {alpha_date}, {age} days before {trade_date}. "
            f"Refresh the data (limit is {MAX_ALPHA_AGE_DAYS} days)."
        )

    return alpha_date


def load_alphas(alpha_date: dt.date, signals: list[str]) -> pl.DataFrame:
    """One row per barrid with one alpha column per signal."""
    lazy = _scan_alphas(alpha_date.year)
    if lazy is None:
        raise DataError(f"No alpha file for {alpha_date.year}")

    alphas = lazy.filter(pl.col("date").eq(alpha_date)).collect()

    available = alphas["signal_name"].unique().sort().to_list()
    if missing := set(signals) - set(available):
        raise DataError(
            f"No alphas on {alpha_date} for {sorted(missing)}. Available: {available}"
        )

    return (
        alphas.filter(pl.col("signal_name").is_in(signals))
        .pivot(index="barrid", on="signal_name", values="alpha")
        .select("barrid", *signals)
        .sort("barrid")
    )


def load_assets(alpha_date: dt.date) -> dy.DataFrame[Assets]:
    assets = sfd.load_assets_by_date(
        date_=alpha_date,
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
        raise DataError(f"No assets on {alpha_date}")

    return Assets.validate(assets)


def load_exposure_barrids(alpha_date: dt.date) -> list[str]:
    return sfd.load_exposures_by_date(alpha_date)["barrid"].to_list()


def load_covariance_matrix(alpha_date: dt.date, barrids: list[str]) -> np.ndarray:
    """Covariance in decimal space, with rows and columns in the order of sorted barrids."""
    return (
        sfd.construct_covariance_matrix(date_=alpha_date, barrids=sorted(barrids))
        .drop("barrid")
        .to_numpy()
    )
