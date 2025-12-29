import polars as pl

from components.models import SignalCombinator


# Define combinators as factory functions
def mean_combinator() -> SignalCombinator:
    """Average all signals"""
    return SignalCombinator(
        name="mean",
        combine_fn=lambda signal_names: pl.mean_horizontal(signal_names).alias("alpha"),
    )


# Registry for easy lookup
COMBINATORS = {
    "mean": mean_combinator,
}


def get_combinator(name: str, **kwargs) -> SignalCombinator:
    """
    Get a signal combinator by name

    Args:
        name: Name of the combinator ('mean', 'weighted', 'max', 'min')
        **kwargs: Additional arguments (e.g., weights for weighted combinator)

    Returns:
        SignalCombinator instance
    """
    if name not in COMBINATORS:
        raise ValueError(
            f"Unknown combinator: {name}. Available: {list(COMBINATORS.keys())}"
        )

    return COMBINATORS[name](**kwargs)
