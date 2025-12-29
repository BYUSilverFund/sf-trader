import dataframely as dy
import polars as pl
import numpy as np
import sf_quant.data as sfd
from config import Config
from components.models import Assets, Betas, Prices, Weights
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
        .sort("ticker")
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
        .sort("ticker", "date")
    )

    return Assets.validate(asset_data)


def get_betas(tickers: list[str]) -> dy.DataFrame[Betas]:
    betas = (
        sfd.load_assets_by_date(
            date_=_config.data_date,
            columns=["ticker", "predicted_beta"],
            in_universe=True,
        )
        .filter(pl.col("ticker").is_in(tickers))
        .sort("ticker")
    )

    return Betas.validate(betas)


def get_ticker_barrid_mapping() -> pl.DataFrame:
    mapping = sfd.load_assets_by_date(
        date_=_config.data_date, columns=["ticker", "barrid"], in_universe=True
    )

    return mapping


def get_benchmark_weights() -> dy.DataFrame[Weights]:
    return (
        sfd.load_assets(
            start=_config.data_date,
            end=_config.data_date,
            in_universe=True,
            columns=["date", "ticker", "market_cap"],
        )
        .select(
            "ticker",
            pl.col("market_cap")
            .truediv(pl.col("market_cap").sum())
            .over("date")
            .alias("weight"),
        )
        .sort("ticker")
    )


def get_covariance_matrix(tickers: list[str]) -> np.ndarray:
    ids = (
        get_ticker_barrid_mapping()
        .join(pl.DataFrame({"ticker": tickers}), on="ticker", how="inner")
        .sort("ticker")
    )
    tickers_ = ids["ticker"].to_list()
    barrids = ids["barrid"].to_list()
    sorted_barrids = sorted(barrids)
    mapping = {barrid: ticker for barrid, ticker in zip(barrids, tickers_)}

    covariance_matrix = (
        sfd.construct_covariance_matrix(date_=_config.data_date, barrids=sorted_barrids)
        .with_columns(pl.col("barrid").replace(mapping))
        .rename(mapping | {"barrid": "ticker"})
        .sort("ticker")
    )

    # Sort columns to match row order
    sorted_tickers = covariance_matrix["ticker"].to_list()
    covariance_matrix = covariance_matrix.select(["ticker"] + sorted_tickers)

    return covariance_matrix.drop("ticker").to_numpy()
