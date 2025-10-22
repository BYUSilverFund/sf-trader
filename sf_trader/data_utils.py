import asyncio
import datetime as dt

import dataframely as dy
import polars as pl
import sf_quant.data as sfd
from ib_insync import IB, Stock

from sf_trader.models import AssetData, Betas, Prices, Shares


def get_asset_data(
    tickers: list[str], trade_date: dt.date, lookback_days: int
) -> dy.DataFrame[AssetData]:
    start_date = trade_date - dt.timedelta(days=lookback_days)

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
            start=start_date, end=trade_date, columns=columns, in_universe=True
        )
        .with_columns(pl.col("return").truediv(100))
        .filter(pl.col("ticker").is_in(tickers))
        .sort("barrid", "date")
    )

    return AssetData.validate(asset_data)


def get_tickers(trade_date: dt.date) -> list[str]:
    return (
        sfd.load_assets_by_date(date_=trade_date, columns=["ticker"], in_universe=True)[
            "ticker"
        ]
        .unique()
        .sort()
        .to_list()
    )


def get_barra_prices(trade_date: dt.date) -> dy.DataFrame[Prices]:
    prices = sfd.load_assets_by_date(
        date_=trade_date, columns=["ticker", "price"], in_universe=True
    ).sort("ticker", "price")

    return Prices.validate(prices)


def get_betas(tickers: str, trade_date: dt.date) -> dy.DataFrame[Betas]:
    betas = (
        sfd.load_assets_by_date(
            date_=trade_date,
            columns=["barrid", "ticker", "predicted_beta"],
            in_universe=True,
        )
        .filter(pl.col("ticker").is_in(tickers))
        .sort("barrid")
        .select("barrid", "predicted_beta")
    )

    return Betas.validate(betas)


def get_available_funds() -> float:
    # Create an IB instance
    ib = IB()

    # Connect to TWS or IB Gateway
    # Default ports: TWS live=7496, TWS paper=7497, Gateway live=4001, Gateway paper=4002
    ib.connect("127.0.0.1", 7497, clientId=1)

    # Get account values
    account_values = ib.accountValues()

    available_funds = 0

    for value in account_values:
        match value.tag:
            case "AvailableFunds":
                available_funds = value.value
            case _:
                pass

    ib.disconnect()

    return float(available_funds)


def get_ibkr_prices(tickers: list[str], status=None) -> dy.DataFrame[Prices]:
    if status:
        status.update("[bold blue]Fetching prices from IBKR... Connecting")

    ib = IB()
    ib.connect("127.0.0.1", 7497, clientId=1)

    # Request delayed market data
    ib.reqMarketDataType(3)  # 3 = delayed data, 4 = delayed-frozen
    contracts = [
        Stock(ticker.replace(".", " "), "SMART", "USD") for ticker in tickers
    ]  # IBKR uses "BRK B" not "BRK.B"

    async def get_snapshots_batch(contracts, batch_size=50):
        results = []
        num_batches = (len(contracts) + batch_size - 1) // batch_size
        for i in range(0, len(contracts), batch_size):
            batch = contracts[i : i + batch_size]
            batch_num = i // batch_size + 1
            if status:
                status.update(
                    f"[bold blue]Fetching prices from IBKR... Batch {batch_num}/{num_batches}"
                )
            tickers = [ib.reqMktData(contract, "", True) for contract in batch]
            await asyncio.sleep(2)  # Wait for data to arrive
            results.extend(tickers)
        return results

    # Run it
    snapshots = ib.run(get_snapshots_batch(contracts))

    if status:
        status.update("[bold blue]Fetching prices from IBKR... Converting to DataFrame")

    # Convert to DataFrame
    data = []
    for ticker in snapshots:
        data.append(
            {
                "ticker": ticker.contract.symbol,
                "time": ticker.time,
                "bid": ticker.bid,
                "ask": ticker.ask,
                "last": ticker.last,
                "volume": ticker.volume,
                "open": ticker.open,
                "high": ticker.high,
                "low": ticker.low,
                "close": ticker.close,
                "bid_size": ticker.bidSize,
                "ask_size": ticker.askSize,
                "last_size": ticker.lastSize,
            }
        )

    ib.disconnect()

    prices = pl.DataFrame(data).select(
        "ticker", pl.mean_horizontal("bid", "ask").alias("price")
    )

    return Prices.validate(prices)


def get_ibkr_positions() -> dy.DataFrame[Shares]:
    """Get current positions from IBKR account"""
    # Create an IB instance
    ib = IB()

    # Connect to TWS or IB Gateway
    # Default ports: TWS live=7496, TWS paper=7497, Gateway live=4001, Gateway paper=4002
    ib.connect("127.0.0.1", 7497, clientId=1)

    # Get positions
    positions = ib.positions()

    # Convert to list of dicts
    data = []
    for position in positions:
        # IBKR uses spaces in symbols like "BRK B", convert back to "BRK.B"
        ticker = position.contract.symbol.replace(" ", ".")

        data.append(
            {
                "ticker": ticker,
                "shares": position.position,
            }
        )

    ib.disconnect()

    if not data:
        # Return empty DataFrame with correct schema
        shares = pl.DataFrame(
            {
                "ticker": [],
                "shares": [],
            }
        )
    else:
        shares = pl.DataFrame(data)

    return Shares.validate(shares)
