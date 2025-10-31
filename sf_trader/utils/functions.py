from sf_trader.components.models import Shares
import dataframely as dy
import polars as pl
from sf_trader.config import Config
from sf_trader.components.models import Prices, Assets, Alphas, Betas, Weights, Orders
import sf_quant.optimizer as sfo
import numpy as np

_config = None


def set_config(config: Config) -> None:
    global _config
    _config = config


def get_tradable_universe(prices: dy.DataFrame[Prices]) -> list[str]:
    return (
        prices.filter(
            pl.col("price").ge(5),
        )["ticker"]
        .unique()
        .sort()
        .to_list()
    )


def get_alphas(assets: dy.DataFrame[Assets]) -> dy.DataFrame[Alphas]:
    signals = _config.signals
    signal_combinator = _config.signal_combinator
    ic = _config.ic
    data_date = _config.data_date

    alphas = (
        assets.sort("ticker", "date")
        # Compute signals
        .with_columns([signal.expr for signal in signals])
        # Compute scores
        .with_columns(
            [
                pl.col(signal.name)
                .sub(pl.col(signal.name).mean())
                .truediv(pl.col(signal.name).std())
                for signal in signals
            ]
        )
        # Compute alphas
        .with_columns(
            [
                pl.col(signal.name).mul(pl.lit(ic)).mul(pl.col("specific_risk"))
                for signal in signals
            ]
        )
        # Fill null alphas with 0
        .with_columns(pl.col(signal.name).fill_null(0) for signal in signals)
        # Combine alphas
        .with_columns(signal_combinator.combine_fn([signal.name for signal in signals]))
        # Get trade date
        .filter(pl.col("date").eq(data_date))
        .select("ticker", "alpha")
        .sort("ticker")
    )

    return Alphas.validate(alphas)


def get_optimal_weights(
    tickers: list[str],
    alphas: dy.DataFrame[Alphas],
    betas: dy.DataFrame[Betas],
    covariance_matrix: np.ndarray,
) -> dy.DataFrame[Weights]:
    decimal_places = _config.decimal_places

    tickers = sorted(tickers)
    alphas = alphas.sort("ticker")["alpha"].to_numpy()
    betas = betas.sort("ticker")["predicted_beta"].to_numpy()

    gamma = _config.gamma
    constraints = _config.constraints

    weights = sfo.mve_optimizer(
        ids=tickers,
        alphas=alphas,
        betas=betas,
        covariance_matrix=covariance_matrix,
        constraints=constraints,
        gamma=gamma,
    )

    weights_rounded = (
        weights.rename({"barrid": "ticker"})
        .with_columns(pl.col("weight").round(4))
        .filter(pl.col("weight").ge(1 * 10**-decimal_places))
        .sort("ticker")
    )

    return Weights.validate(weights_rounded)


def get_optimal_shares(
    weights: dy.DataFrame[Weights], prices: dy.DataFrame[Prices], account_value: float
) -> dy.DataFrame[Shares]:
    optimal_shares = (
        weights.join(prices, on="ticker", how="left")
        .with_columns(pl.lit(account_value).mul(pl.col("weight")).alias("dollars"))
        .with_columns(
            pl.col("dollars").truediv(pl.col("price")).floor().alias("shares")
        )
        .select(
            "ticker",
            "shares",
        )
    )

    return Shares.validate(optimal_shares)


def get_order_deltas(
    prices: dy.DataFrame[Prices],
    current_shares: dy.DataFrame[Shares],
    optimal_shares: dy.DataFrame[Shares],
) -> dy.DataFrame[Orders]:
    # Prep shares dataframes for join
    current_shares = current_shares.rename({"shares": "current_shares"})
    optimal_shares = optimal_shares.rename({"shares": "optimal_shares"})

    orders = (
        prices
        # Joins
        .join(current_shares, on="ticker", how="left")
        .join(optimal_shares, on="ticker", how="left")
        # Fill nulls with 0
        .with_columns(pl.col("current_shares", "optimal_shares").fill_null(0))
        # Compute share differential
        .with_columns(pl.col("optimal_shares").sub("current_shares").alias("shares"))
        # Compute order side
        .with_columns(
            pl.when(pl.col("shares").gt(0))
            .then(pl.lit("BUY"))
            .when(pl.col("shares").lt(0))
            .then(pl.lit("SELL"))
            .otherwise(pl.lit("HOLD"))
            .alias("action")
        )
        # Absolute value the shares
        .with_columns(pl.col("shares").abs())
        # Select
        .select("ticker", "price", "shares", "action")
        # Filter
        .filter(
            pl.col("ticker")
            .is_in(_config.ignore_tickers)
            .not_(),  # Ignore problematic tickers
            pl.col("shares").ne(0),  # Remove 0 share trades
            pl.col("action").ne("HOLD"),  # Remove HOLDs
            pl.col("price").is_not_null(),  # Remove unknown prices
        )
        # Sort
        .sort("ticker")
    )

    return Orders.validate(orders)
