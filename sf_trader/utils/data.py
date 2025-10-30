import dataframely as dy
import polars as pl
import numpy as np
import sf_quant.data as sfd
from sf_trader.config import Config
from sf_trader.components.models import Assets, Betas, Prices, Weights
import datetime as dt

_config = None


def set_config(config: Config) -> None:
    global _config
    _config = config


def get_universe() -> list[str]:
    return (
        sfd.load_assets_by_date(
            date_=_config.data_date, columns=["ticker"], in_universe=True
        )["ticker"]
        .unique()
        .sort()
        .to_list()
    )


def get_prices(tickers: list[str]) -> dy.DataFrame[Prices]:
    prices = (
        sfd.load_assets_by_date(
            date_=_config.data_date, columns=["ticker", "price"], in_universe=True
        )
        .filter(pl.col("ticker").is_in(tickers))
        .sort("ticker", "price")
    )

    return Prices.validate(prices)


def get_assets(tickers: list[str]) -> dy.DataFrame[Assets]:
    lookback_days = max([signal.lookback_days for signal in _config.signals])
    start_date = _config.data_date - dt.timedelta(days=lookback_days)
    end_date = _config.data_date

    columns = [
        "date",
        "barrid",
        "ticker",
        "return",
        "predicted_beta",
        "specific_risk",
    ]

    asset_data = (
        sfd.load_assets(
            start=start_date, end=end_date, columns=columns, in_universe=True
        )
        .with_columns(pl.col("return").truediv(100))
        .filter(pl.col("ticker").is_in(tickers))
        .sort("barrid", "date")
    )

    return Assets.validate(asset_data)


def get_betas(tickers: list[str]) -> dy.DataFrame[Betas]:
    betas = (
        sfd.load_assets_by_date(
            date_=_config.data_date,
            columns=["barrid", "ticker", "predicted_beta"],
            in_universe=True,
        )
        .filter(pl.col("ticker").is_in(tickers))
        .sort("barrid")
        .select("barrid", "predicted_beta")
    )

    return Betas.validate(betas)


def get_ticker_barrid_mapping() -> pl.DataFrame:
    mapping = sfd.load_assets_by_date(
        date_=_config.data_date, columns=["ticker", "barrid"], in_universe=True
    )

    return mapping


def get_benchmark_weights() -> dy.DataFrame[Weights]:
    return (
        sfd.load_benchmark(start=_config.data_date, end=_config.data_date)
        .sort("barrid")
    )


def get_covariance_matrix(barrids: list[str]) -> np.ndarray:
    return (
        sfd.construct_covariance_matrix(date_=_config.data_date, barrids=barrids)
        .drop("barrid")
        .to_numpy()
    )
