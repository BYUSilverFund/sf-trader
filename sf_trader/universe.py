import dataframely as dy
import polars as pl

from sf_trader.models import Assets


def apply_universe_filter(
    alphas: pl.DataFrame,
    assets: dy.DataFrame[Assets],
    min_price: float,
    exposure_barrids: list[str],
) -> tuple[pl.DataFrame, dict[str, int]]:
    """Keep Russell 3000 names priced above min_price that the optimizer has data for.

    Returns the filtered alphas joined to asset data, and the number of names
    dropped by each rule. A name failing several rules counts under the first.
    """
    rules = {
        "not_in_russell_3000": pl.col("in_universe").fill_null(False),
        "price_at_or_below_min": pl.col("price").gt(min_price).fill_null(False),
        "no_ticker": pl.col("ticker").is_not_null(),
        "no_risk_data": pl.col("specific_risk").is_not_null()
        & pl.col("predicted_beta").is_not_null()
        & pl.col("barrid").is_in(exposure_barrids),
    }

    filtered = alphas.join(assets, on="barrid", how="left")
    dropped = {}
    for name, keep in rules.items():
        remaining = filtered.filter(keep)
        dropped[name] = filtered.height - remaining.height
        filtered = remaining

    return filtered.sort("barrid"), dropped


def apply_do_not_trade(
    alphas: pl.DataFrame, do_not_trade: list[str]
) -> tuple[pl.DataFrame, int]:
    """Remove do-not-trade tickers so the optimizer can never allocate to them."""
    remaining = alphas.filter(pl.col("ticker").is_in(do_not_trade).not_())
    return remaining, alphas.height - remaining.height
