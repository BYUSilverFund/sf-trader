import polars as pl

from sf_trader.components.models import Signal


def momentum() -> Signal:
    return Signal(
        name="momentum",
        expr=(
            pl.col("return")
            .log1p()
            .rolling_sum(window_size=230)
            .shift(22)
            .over("barrid")
            .alias("momentum")
        ),
        lookback_days=252,
    )

def vol_scaled_barra_momentum() -> Signal:
    return Signal(
        name="vol_scaled_barra_momentum",
        expr=(
            pl.col("specific_return")
            .log1p()
            .rolling_sum(230)
            .truediv(pl.col("specific_return").rolling_std(230))
            .shift(21)
            .over("barrid")
            .alias("vol_scaled_barra_momentum")
        ),
        lookback_days=500,
    )

def reversal() -> Signal:
    return Signal(
        name="reversal",
        expr=(
            pl.col("return")
            .log1p()
            .rolling_sum(window_size=22)
            .mul(-1)
            .over("barrid")
            .alias("reversal")
        ),
        lookback_days=22,
    )

def barra_reversal_volume_clipped() -> Signal:
    return Signal(
        name="barra_reversal_volume_clipped",
        expr=(
            pl.col("specific_return")
            .ewm_mean(span=5, min_samples=5)
            .mul(-1)
            .shift(1)
            .over("barrid")
            .alias("barra_reversal_volume_clipped")
        ),
        lookback_days=252,
    )

def ivol() -> Signal:
    return Signal(
        name="ivol",
        expr=(
            pl.col("specific_risk")
            .mul(-1)
            .shift(1)
            .over("barrid")
            .alias("ivol")
        ),
        lookback_days=0,
    )

def beta() -> Signal:
    return Signal(
        name="beta",
        expr=(pl.col("predicted_beta").mul(-1).over("barrid").alias("beta")),
        lookback_days=0,
    )


# Registry for easy lookup
SIGNALS = {
    "momentum": momentum(),
    "reversal": reversal(),
    "beta": beta(),
    "vol_scaled_barra_momentum": vol_scaled_barra_momentum(),
    "barra_reversal_volume_clipped": barra_reversal_volume_clipped(),
    "ivol": ivol(),
}


def get_signal(name: str) -> Signal:
    """
    Get a signal by name

    Args:
        name: Name of the signal ('momentum', 'reversal', 'beta')

    Returns:
        Signal instance

    Raises:
        ValueError: If signal name is not found
    """
    if name not in SIGNALS:
        raise ValueError(f"Unknown signal: {name}. Available: {list(SIGNALS.keys())}")

    return SIGNALS[name]
