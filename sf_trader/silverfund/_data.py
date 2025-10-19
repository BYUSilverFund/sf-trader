import datetime as dt
import polars as pl
import sf_quant.data as sfd

def get_asset_data(trade_date: dt.date, lookback_days: int) -> pl.DataFrame:
    start_date = trade_date - dt.timedelta(days=lookback_days)

    columns = [
        'date',
        'barrid',
        'ticker',
        'return',
        'predicted_beta',
        'specific_risk',
    ]

    return (
        sfd.load_assets(
            start=start_date,
            end=trade_date,
            columns=columns,
            in_universe=True
        )
        .with_columns(
            pl.col('return').truediv(100)
        )
    )

def get_tickers(trade_date: dt.date) -> pl.DataFrame:
    return (
        sfd.load_assets_by_date(
            date_=trade_date,
            columns=['tickers'],
            in_universe=True
        )
        .with_columns(
            pl.col('return').truediv(100)
        )
    )