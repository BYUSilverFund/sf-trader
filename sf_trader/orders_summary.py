import dataframely as dy
import polars as pl
from sf_trader.components.models import Orders, Shares, Prices
from sf_trader.config import Config
from rich.console import Console
import sf_trader.ui.tables
import sf_trader.utils.data


def get_orders_summary(
    shares: dy.DataFrame[Shares], orders: dy.DataFrame[Orders], config: Config
) -> None:
    """
    Generate and display orders summary tables.

    Args:
        shares: DataFrame with ticker and optimal shares columns
        orders: DataFrame with ticker, price, shares, action columns
        config: Configuration object
    """
    # Configure modules
    sf_trader.utils.data.set_config(config=config)

    # Connect to broker and get current positions
    broker = config.broker
    current_shares = broker.get_positions()

    # Compute ticker list from both current and optimal portfolios
    tickers = list(set(current_shares["ticker"].to_list() + shares["ticker"].to_list()))

    # Get prices for all tickers
    prices = sf_trader.utils.data.get_prices(tickers=tickers)

    # Create combined shares dataframe with both current and optimal shares
    combined_shares = get_combined_shares(
        current_shares=current_shares, optimal_shares=shares, config=config
    )

    # Get top 10 long positions from current shares
    top_long_orders = get_top_long_orders(
        shares=combined_shares, prices=prices, orders=orders, top_n=10
    )
    print(top_long_orders)
    top_long_orders_table = sf_trader.ui.tables.generate_orders_table(
        orders=top_long_orders, title="Top 10 Long Position Orders"
    )

    # Get top 10 active BUY orders by dollar value
    top_active_buy_orders = get_top_active_orders(
        shares=combined_shares, orders=orders, prices=prices, action="BUY", top_n=10
    )
    top_active_buy_orders_table = sf_trader.ui.tables.generate_orders_table(
        orders=top_active_buy_orders, title="Top 10 Active BUY Orders by Dollar Value"
    )

    # Get top 10 active SELL orders by dollar value
    top_active_sell_orders = get_top_active_orders(
        shares=combined_shares, orders=orders, prices=prices, action="SELL", top_n=10
    )
    top_active_sell_orders_table = sf_trader.ui.tables.generate_orders_table(
        orders=top_active_sell_orders, title="Top 10 Active SELL Orders by Dollar Value"
    )

    # Render UI
    console = Console()
    console.print()
    console.print(top_long_orders_table)
    console.print()
    console.print(top_active_buy_orders_table)
    console.print()
    console.print(top_active_sell_orders_table)

    del broker
    del config.broker


def get_top_long_orders(
    shares: dy.DataFrame[Shares],
    prices: dy.DataFrame[Prices],
    orders: dy.DataFrame[Orders],
    top_n: int = 10,
) -> pl.DataFrame:
    long_positions = (
        shares.join(prices, on="ticker", how="left")
        .join(
            orders.select("ticker", pl.col("shares").alias("to_trade"), "action"),
            on="ticker",
            how="left",
        )
        .with_columns(
            pl.col("action").fill_null("HOLD"),
            pl.col("to_trade").fill_null(0),
            pl.when(pl.col("price").is_null())
            .then(pl.lit(9999))
            .otherwise(pl.col("price"))
            .alias("price"),
        )
        .with_columns(
            (pl.col("shares") * pl.col("price")).alias("dollars"),
        )
        .filter(pl.col("shares") > 0)  # Only long positions
        .sort("dollars", descending=True)
        .head(top_n)
        .select("ticker", "shares", "price", "dollars", "to_trade", "action")
    )

    return long_positions


def get_top_active_orders(
    shares: dy.DataFrame[Shares],
    orders: dy.DataFrame[Orders],
    prices: dy.DataFrame[Prices],
    action: str,
    top_n: int = 10,
) -> pl.DataFrame:
    active_orders = (
        shares.join(prices, on="ticker", how="left")
        .join(
            orders.select("ticker", pl.col("shares").alias("to_trade"), "action"),
            on="ticker",
            how="left",
        )
        .with_columns(
            pl.col("action").fill_null("HOLD"),
            pl.col("to_trade").fill_null(0),
            pl.when(pl.col("price").is_null())
            .then(pl.lit(9999))
            .otherwise(pl.col("price"))
            .alias("price"),
        )
        .with_columns(
            (pl.col("shares") * pl.col("price")).alias("dollars"),
        )
        .filter(
            pl.col("action").eq(action),  # Filter by specific action (BUY or SELL)
        )
        .sort("dollars", descending=True)
        .head(top_n)
        .select("ticker", "shares", "price", "dollars", "to_trade", "action")
    )

    return active_orders


def get_combined_shares(
    current_shares: dy.DataFrame[Shares],
    optimal_shares: dy.DataFrame[Shares],
    config: Config,
) -> dy.DataFrame[Shares]:
    # Get all unique tickers from both dataframes
    all_tickers = list(
        set(current_shares["ticker"].to_list() + optimal_shares["ticker"].to_list())
        - set(config.ignore_tickers)
    )

    # Create a dataframe with all tickers
    all_tickers_df = pl.DataFrame({"ticker": all_tickers})

    # Join with current shares to get actual holdings
    combined = all_tickers_df.join(
        current_shares, on="ticker", how="left"
    ).with_columns(
        pl.col("shares").fill_null(0),
    )
    print(combined)

    return Shares.validate(combined)
