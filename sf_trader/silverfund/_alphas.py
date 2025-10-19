import polars as pl
import sf_quant.data as sfd
import sf_quant.optimizer as sfo
import datetime as dt
import numpy as np
from ._signals import Signal

def get_alphas(df: pl.DataFrame, signals: list[Signal], ic: float) -> pl.DataFrame:
    return (
        df
        .sort('barrid', 'date')
        # Compute signals
        .with_columns([signal.expr for signal in signals])
        # Compute scores
        .with_columns([
            pl.col(signal.name).sub(pl.col(signal.name).mean()).truediv(pl.col(signal.name).std())
            for signal in signals
        ])
        # Compute alphas
        .with_columns([
            pl.col(signal.name).mul(pl.lit(ic)).mul(pl.col('specific_risk'))
            for signal in signals
        ])
        # Fill null alphas with 0
        .with_columns(
            pl.col(signal.name).fill_null(0)
            for signal in signals    
        )
        # Combine alphas
        .with_columns(
            pl.mean_horizontal([signal.name for signal in signals]).alias('alpha')
        )
        .select(
            'date',
            'barrid',
            'ticker',
            'predicted_beta',
            'alpha'
        )
        .sort('barrid', 'date')

    )

def apply_filters(data: pl.DataFrame, trade_date: dt.date) -> pl.DataFrame:
    return (
        data
        .filter(
            pl.col('date').eq(trade_date)
        )
        .sort('barrid', 'date')
    )



def get_prices(trade_date: dt.date) -> pl.DataFrame:
    return (
        sfd.load_assets_by_date(
            date_=trade_date,
            in_universe=True,
            columns=['ticker', 'price']
        )
    )