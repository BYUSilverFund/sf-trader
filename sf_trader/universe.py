import polars as pl


def apply_do_not_trade(
    holdings: pl.DataFrame, do_not_trade: list[str]
) -> tuple[pl.DataFrame, int]:
    """Remove do-not-trade tickers so they never get an order."""
    remaining = holdings.filter(pl.col("ticker").is_in(do_not_trade).not_())
    return remaining, holdings.height - remaining.height
