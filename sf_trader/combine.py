from typing import Callable

import polars as pl


def equal_weight(alphas: pl.DataFrame, signals: list[str]) -> pl.DataFrame:
    """Average the signals. A signal missing for a name contributes zero, as in the backtests."""
    return alphas.with_columns(
        pl.mean_horizontal(pl.col(signals).fill_null(0.0)).alias("combined_alpha")
    )


STRATEGIES: dict[str, Callable[[pl.DataFrame, list[str]], pl.DataFrame]] = {
    "equal_weight": equal_weight,
}


def combine_alphas(
    alphas: pl.DataFrame, signals: list[str], strategy: str
) -> pl.DataFrame:
    return STRATEGIES[strategy](alphas, signals)
