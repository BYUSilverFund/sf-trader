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



def get_prices(tickers: list[str]) -> dy.DataFrame[Prices]:
    prices = (
        sfd.load_assets_by_date(
            date_=_config.data_date, columns=["ticker", "price"], in_universe=True
        )
        .filter(pl.col("ticker").is_in(tickers))
        .sort("ticker")
    )

    return Prices.validate(prices)





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