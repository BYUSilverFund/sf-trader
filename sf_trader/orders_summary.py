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

    # Get tickers
    tickers = shares["ticker"].to_list()

    # Get prices
    prices = sf_trader.utils.data.get_prices(tickers=tickers)

    # Get top 10 long positions from optimal shares
    top_long_orders = get_top_long_orders(
        shares=shares, prices=prices, orders=orders, top_n=10
    )
    print(top_long_orders)
    top_long_orders_table = sf_trader.ui.tables.generate_orders_table(
        orders=top_long_orders, title="Top 10 Long Position Orders"
    )

    # Get top 10 non-HOLD orders by dollar value
    top_active_orders = get_top_active_orders(
        shares=shares, orders=orders, prices=prices, top_n=10
    )
    top_active_orders_table = sf_trader.ui.tables.generate_orders_table(
        orders=top_active_orders, title="Top 10 Active Orders by Dollar Value"
    )

    # Render UI
    console = Console()
    console.print()
    console.print(top_long_orders_table)
    console.print()
    console.print(top_active_orders_table)

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
            (pl.col("shares") * pl.col("price")).alias("dollars"),
            pl.col("action").fill_null("HOLD"),
            pl.col("to_trade").fill_null(0),
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
            (pl.col("shares") * pl.col("price")).alias("dollars"),
            pl.col("action").fill_null("HOLD"),
            pl.col("to_trade").fill_null(0),
        )
        .filter(
            pl.col("shares") > 0,  # Only long positions
            pl.col("action").ne("HOLD"),  # Only active positions
        )
        .sort("dollars", descending=True)
        .head(top_n)
        .select("ticker", "shares", "price", "dollars", "to_trade", "action")
    )

    return active_orders
