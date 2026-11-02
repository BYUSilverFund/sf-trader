import math
from pathlib import Path

import dataframely as dy
import polars as pl

from sf_trader.config import OrderSettings
from sf_trader.ibkr import to_ibkr_symbol
from sf_trader.models import Orders, Portfolio, Positions, Quotes

SHARE_DECIMALS = 4
DEFAULT_TICK = 0.01
BASKET_COLUMNS = [
    "Action",
    "Quantity",
    "Symbol",
    "SecType",
    "Exchange",
    "Currency",
    "TimeInForce",
    "OrderType",
    "LmtPrice",
    "BasketTag",
]


def round_to_tick(price: float, tick: float, side: str, toward_market: bool = False) -> float:
    """Round to a valid tick.

    By default a buy rounds down and a sell rounds up, so rounding never costs
    anything. With toward_market the direction flips, which gets stalled orders
    filled on one-tick spreads, where any other rounding lands back on the near touch.
    """
    steps = price / tick
    round_down = (side == "BUY") != toward_market
    steps = math.floor(steps + 1e-9) if round_down else math.ceil(steps - 1e-9)
    decimals = max(0, -math.floor(math.log10(tick)))
    return round(steps * tick, decimals)


def limit_price(
    side: str,
    bid: float,
    ask: float,
    aggressiveness: float,
    tick: float,
    toward_market: bool = False,
) -> float:
    """Pay the configured fraction of the half-spread beyond mid, staying inside the quote."""
    mid = (bid + ask) / 2
    if side == "BUY":
        price = mid + aggressiveness * (ask - mid)
    else:
        price = mid - aggressiveness * (mid - bid)
    return round_to_tick(price, tick, side, toward_market)


def resolve_prices(
    quotes: dy.DataFrame[Quotes], settings: OrderSettings
) -> pl.DataFrame:
    """Add a mid reference price. Crossed or one-sided quotes count as missing."""
    two_sided = pl.col("bid").is_not_null() & pl.col("ask").is_not_null() & pl.col(
        "bid"
    ).le(pl.col("ask"))
    fallback = (
        pl.coalesce("last", "close")
        if settings.quote_fallback == "last"
        else pl.lit(None, dtype=pl.Float64)
    )
    return quotes.with_columns(
        pl.when(two_sided).then(pl.col("bid")).otherwise(fallback).alias("bid"),
        pl.when(two_sided).then(pl.col("ask")).otherwise(fallback).alias("ask"),
    ).with_columns(
        pl.col("bid").add(pl.col("ask")).truediv(2).alias("reference_price"),
        pl.col("min_tick").fill_null(DEFAULT_TICK),
    )


def compute_targets(
    portfolio: dy.DataFrame[Portfolio],
    quotes: dy.DataFrame[Quotes],
    settings: OrderSettings,
    capital: float,
) -> dy.DataFrame[Portfolio]:
    """Fix target shares at current mids. Names without a quote keep their close-based estimate."""
    priced = resolve_prices(quotes, settings).select("ticker", "reference_price")
    return Portfolio.validate(
        portfolio.join(priced, on="ticker", how="left")
        .with_columns(
            pl.col("weight")
            .mul(capital)
            .truediv(pl.col("reference_price"))
            .round(SHARE_DECIMALS)
            .fill_null(pl.col("target_shares"))
            .alias("target_shares")
        )
        .drop("reference_price")
    )


def compute_orders(
    portfolio: dy.DataFrame[Portfolio],
    positions: dy.DataFrame[Positions],
    quotes: dy.DataFrame[Quotes],
    settings: OrderSettings,
    do_not_trade: list[str],
    toward_market: bool = False,
) -> tuple[dy.DataFrame[Orders], list[str], int]:
    """Diff target shares against current holdings.

    Returns the orders, the tickers skipped for lack of a usable quote, and the
    number of trades skipped for being below min_trade_value.
    """
    priced = resolve_prices(quotes, settings)
    targets = portfolio.select("ticker", pl.col("target_shares").alias("target"))
    holdings = positions.rename({"shares": "held"})

    trades = (
        targets.join(holdings, on="ticker", how="full", coalesce=True)
        .join(priced, on="ticker", how="left")
        .filter(pl.col("ticker").is_in(do_not_trade).not_())
        .with_columns(pl.col("target", "held").fill_null(0.0))
        .with_columns(
            pl.col("target").sub(pl.col("held")).round(SHARE_DECIMALS).alias("delta")
        )
        .filter(pl.col("delta").ne(0))
    )

    skipped_no_quote = (
        trades.filter(pl.col("reference_price").is_null())["ticker"].sort().to_list()
    )

    orders = (
        trades.filter(pl.col("reference_price").is_not_null())
        .with_columns(
            pl.when(pl.col("delta").gt(0))
            .then(pl.lit("BUY"))
            .otherwise(pl.lit("SELL"))
            .alias("action"),
            pl.col("delta").abs().alias("order_shares"),
        )
        .with_columns(
            pl.col("order_shares").mul("reference_price").alias("value"),
        )
    )

    small = orders.filter(pl.col("value").lt(settings.min_trade_value)).height
    orders = orders.filter(pl.col("value").ge(settings.min_trade_value))

    limits = [
        limit_price(
            row["action"],
            row["bid"],
            row["ask"],
            settings.limit_aggressiveness,
            row["min_tick"],
            toward_market,
        )
        for row in orders.iter_rows(named=True)
    ]

    orders = (
        orders.with_columns(pl.Series("limit_price", limits, dtype=pl.Float64))
        .select(
            "ticker",
            "action",
            pl.col("order_shares").alias("shares"),
            "limit_price",
            "value",
            "primary_exchange",
        )
        .sort("ticker")
    )

    return Orders.validate(orders), skipped_no_quote, small


def _format_number(value: float, decimals: int) -> str:
    return f"{value:.{decimals}f}".rstrip("0").rstrip(".")


def to_basket(orders: dy.DataFrame[Orders], tag: str) -> pl.DataFrame:
    """Orders in the BasketTrader import layout."""
    rows = [
        {
            "Action": row["action"],
            "Quantity": _format_number(row["shares"], SHARE_DECIMALS),
            "Symbol": to_ibkr_symbol(row["ticker"]),
            "SecType": "STK",
            "Exchange": (
                f"SMART/{row['primary_exchange']}"
                if row["primary_exchange"]
                else "SMART"
            ),
            "Currency": "USD",
            "TimeInForce": "DAY",
            "OrderType": "LMT",
            "LmtPrice": _format_number(row["limit_price"], 4),
            "BasketTag": tag,
        }
        for row in orders.iter_rows(named=True)
    ]
    return pl.DataFrame(rows, schema={column: pl.String for column in BASKET_COLUMNS})


def write_basket(orders: dy.DataFrame[Orders], path: Path, tag: str) -> None:
    to_basket(orders, tag).write_csv(path, quote_style="never")
