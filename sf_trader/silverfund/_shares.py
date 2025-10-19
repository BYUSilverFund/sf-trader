import polars as pl

def get_optimal_shares(weights: pl.DataFrame, prices: pl.DataFrame, available_funds: float) -> pl.DataFrame:
    return (
        weights
        .join(
            prices,
            on='ticker',
            how='left'
        )
        .with_columns(
            pl.lit(available_funds).mul(pl.col('weight')).alias('dollars')
        )
        .with_columns(
            pl.col('dollars').truediv(pl.col('price')).alias('shares')
        )
        .with_columns(
            pl.col('shares').floor().alias('shares_rounded')
        )
        .with_columns(
            pl.col('price').mul('shares_rounded').alias('dollars_allocated')
        )
    )