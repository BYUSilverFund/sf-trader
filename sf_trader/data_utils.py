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

def get_ibkr_prices(tickers: list[str]) -> pl.DataFrame:
    from ib_insync import IB, Stock
    import asyncio

    ib = IB()
    ib.connect('127.0.0.1', 7497, clientId=1)

    # Request delayed market data
    ib.reqMarketDataType(3)  # 3 = delayed data, 4 = delayed-frozen
    tickers = [Stock(ticker.replace('.', ' '), 'SMART', 'USD') for ticker in tickers]

    async def get_snapshots_batch(contracts, batch_size=50):
        results = []
        for i in range(0, len(contracts), batch_size):
            batch = contracts[i:i+batch_size]
            tickers = [ib.reqMktData(contract, '', True) for contract in batch]
            await asyncio.sleep(2)  # Wait for data to arrive
            results.extend(tickers)
        return results

    # Run it
    snapshots = ib.run(get_snapshots_batch(tickers))

    # Convert to DataFrame
    data = []
    for ticker in snapshots:
        data.append({
            'symbol': ticker.contract.symbol,
            'time': ticker.time,
            'bid': ticker.bid,
            'ask': ticker.ask,
            'last': ticker.last,
            'volume': ticker.volume,
            'open': ticker.open,
            'high': ticker.high,
            'low': ticker.low,
            'close': ticker.close,
            'bid_size': ticker.bidSize,
            'ask_size': ticker.askSize,
            'last_size': ticker.lastSize,
        })

    return pl.DataFrame(data)
