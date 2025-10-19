import datetime as dt
import polars as pl
import sf_quant.data as sfd
from ib_insync import IB

def get_asset_data(tickers: list[str], trade_date: dt.date, lookback_days: int) -> pl.DataFrame:
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
        .filter(
            pl.col('ticker').is_in(tickers)
        )
        .sort('barrid', 'date')
    )

def get_tickers(trade_date: dt.date) -> pl.DataFrame:
    return (
        sfd.load_assets_by_date(
            date_=trade_date,
            columns=['ticker'],
            in_universe=True
        )
        ['ticker']
        .unique()
        .sort()
        .to_list()
    )

def get_barra_prices(trade_date: dt.date) -> pl.DataFrame:
    return (
        sfd.load_assets_by_date(
            date_=trade_date,
            columns=['ticker', 'price'],
            in_universe=True
        )
        .sort('ticker', 'price')
    )

def get_available_funds():
    # Create an IB instance
    ib = IB()

    # Connect to TWS or IB Gateway
    # Default ports: TWS live=7496, TWS paper=7497, Gateway live=4001, Gateway paper=4002
    ib.connect('127.0.0.1', 7497, clientId=1)

    # Get account values
    account_values = ib.accountValues()

    available_funds = 0

    for value in account_values:
        match value.tag:
            case 'AvailableFunds':
                available_funds = value.value
            case _:
                pass

    ib.disconnect()

    return float(available_funds)