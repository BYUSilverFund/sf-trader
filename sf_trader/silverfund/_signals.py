import polars as pl
from dataclasses import dataclass

@dataclass
class Signal:
    name: str
    expr: pl.Expr
    lookback_days: int

momentum = Signal(
        name='momentum',
        expr=(
            pl.col('return')
            .log1p()
            .rolling_sum(window_size=230)
            .shift(22)
            .over('barrid')
            .alias('momentum')
        ),
        lookback_days=252
    )

reversal = Signal(
    name='reversal',
    expr=(
        pl.col('return')
        .log1p()
        .rolling_sum(window_size=22)
        .mul(-1)
        .over('barrid')
        .alias('reversal')
    ),
    lookback_days=22
)

beta = Signal(
    name='beta',
    expr=(
    pl.col('predicted_beta')
    .mul(-1)
    .over('barrid')
    .alias('beta')
    ),
    lookback_days=0
)