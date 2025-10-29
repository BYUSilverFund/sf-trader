from sf_trader.components.models import Shares
import dataframely as dy
import polars as pl
import sf_quant.data as sfd
from sf_trader.config import Config
from sf_trader.components.models import Prices, Assets, Alphas, Betas, Weights, Orders
import sf_quant.optimizer as sfo
import sf_trader.utils.data

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
        assets.sort("barrid", "date")
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
        .select("barrid", "alpha")
        .sort("barrid")
    )

    return Alphas.validate(alphas)


def _compute_optimal_weights(
    barrids: list[str],
    alphas: dy.DataFrame[Alphas],
    betas: dy.DataFrame[Betas],
) -> pl.DataFrame:
    barrids = sorted(barrids)
    alphas = alphas.sort("barrid")["alpha"].to_numpy()
    betas = betas.sort("barrid")["predicted_beta"].to_numpy()

    gamma = _config.gamma
    constraints = _config.constraints
    data_date = _config.data_date

    covariance_matrix = (
        sfd.construct_covariance_matrix(date_=data_date, barrids=barrids)
        .drop("barrid")
        .to_numpy()
    )

    return sfo.mve_optimizer(
        ids=barrids,
        alphas=alphas,
        betas=betas,
        covariance_matrix=covariance_matrix,
        gamma=gamma,
        constraints=constraints,
    )


def get_optimal_weights(
    tickers: list[str],
    alphas: dy.DataFrame[Alphas],
    betas: dy.DataFrame[Betas],
) -> dy.DataFrame[Weights]:
    decimal_places = _config.decimal_places

    mapping = sf_trader.utils.data.get_ticker_barrid_mapping()
    barrids = (
        mapping.join(other=pl.DataFrame({"ticker": tickers}), how="inner", on="ticker")[
            "barrid"
        ]
        .unique()
        .sort()
        .to_list()
    )

    weights = _compute_optimal_weights(
        barrids=barrids,
        alphas=alphas,
        betas=betas,
    )

    weights_re_keyed = weights.join(other=mapping, on="barrid", how="left").select(
        "ticker", "weight"
    )

    weights_rounded = (
        weights_re_keyed.with_columns(pl.col("weight").round(4))
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
