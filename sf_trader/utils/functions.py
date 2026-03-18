from components.models import Shares
import dataframely as dy
import polars as pl
from config import Config
from components.models import (
    Prices,
    Assets,
    Alphas,
    Betas,
    Weights,
    Orders,
    Dollars,
    PortfolioMetrics,
)
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

def clip_scores_winsorize(signal_name: str) -> pl.Expr:
    if signal_name == "barra_reversal_volume_clipped":
        return pl.col(signal_name).clip(-2.0, 2.0)
    return pl.col(signal_name)

def barra_reversal_volume_clipped_alpha(signal_name: str, ic: float) -> pl.Expr:
    score = pl.col(signal_name).clip(lower_bound=-2.0, upper_bound=2.0)

    dv = (pl.col("daily_volume") * pl.col("price")).log1p()

    dv_mean = dv.rolling_mean(window_size=252, min_samples=1).over("barrid")
    dv_std = dv.rolling_std(window_size=252, min_samples=2).over("barrid")

    volume_score = (
        (dv - dv_mean)
        / dv_std.fill_null(1.0).clip(lower_bound=0.0001)
    ).fill_null(0.0)

    gk_alpha = score * pl.lit(ic) * pl.col("specific_risk")

    alpha = (
        pl.when((score >= 2.0) & (volume_score >= 2.0))
        .then(0.0)
        .otherwise(gk_alpha)
    )

    return alpha.alias(signal_name)


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
                .sub(pl.col(signal.name).mean().over("date"))
                .truediv(
                    pl.col(signal.name)
                    .std().over("date")
                    .fill_null(0.0)
                    .clip(lower_bound=1e-8)
                )
                for signal in signals
            ]
        )
        # Compute alphas
        .with_columns(
            [
                barra_reversal_volume_clipped_alpha(signal.name, ic)
                if signal.name == "barra_reversal_volume_clipped"
                else pl.col(signal.name).mul(pl.lit(ic)).mul(pl.col("specific_risk"))
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


def compute_risk(weights: np.ndarray, covariance_matrix: np.ndarray) -> float:
    return np.sqrt(weights @ covariance_matrix @ weights.T)


def get_dollars(
    shares: dy.DataFrame[Shares], prices: dy.DataFrame[Prices]
) -> dy.DataFrame[Dollars]:
    dollars = (
        shares.join(prices, on="ticker", how="left")
        .with_columns(pl.col("shares").mul("price").alias("dollars"))
        .select("ticker", "dollars")
    )

    return Dollars.validate(dollars)


def get_weights_from_dollars(
    dollars: dy.DataFrame[Dollars], account_value: float
) -> dy.DataFrame[Weights]:
    weights = dollars.with_columns(
        (pl.col("dollars") / pl.lit(account_value)).alias("weight")
    ).sort("ticker")

    return Weights.validate(weights)


def decompose_weights(
    benchmark: dy.DataFrame[Weights], weights: dy.DataFrame[Weights]
) -> tuple[np.ndarray]:
    decomposed_weights = (
        benchmark.select("ticker", pl.col("weight").alias("weight_bmk"))
        .join(
            weights,
            on="ticker",
            how="left",
        )
        .with_columns(
            pl.col("weight").fill_null(0),
        )
        .with_columns(pl.col("weight").sub(pl.col("weight_bmk")).alias("weight_act"))
        .sort("ticker")
    )

    total_weights = decomposed_weights["weight"].to_numpy()
    active_weights = decomposed_weights["weight_act"].to_numpy()

    return total_weights, active_weights


def compute_mcar_pcar(
    active_weights: np.ndarray,
    covariance_matrix: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Computes Marginal Contribution and Percentage Contribution to AR.
    
    Args:
        active_weights (np.ndarray): 
            1D array of active portfolio weights.
        covariance_matrix (np.ndarray): 
            Covariance matrix of asset returns.

    Returns:
        tuple[np.ndarray, np.ndarray, np.ndarray]:
            - mcar: Marginal contribution to active risk
            - tcar: Total contribution to active risk 
            - pcar: Percentage contribution to active risk,
    """
    active_risk = compute_risk(active_weights, covariance_matrix)

    cov_w = covariance_matrix @ active_weights

    mcar = cov_w / active_risk
    tcar = active_weights * mcar
    pcar = tcar / active_risk
    
    return mcar, tcar, pcar


def get_portfolio_metrics(
    total_weights: np.ndarray,
    active_weights: np.ndarray,
    covariance_matrix: np.ndarray,
    account_value: float,
    dollars_allocated: float,
) -> PortfolioMetrics:
    # Compute metrics
    gross_exposure = np.sum(np.abs(total_weights))
    net_exposure = np.sum(total_weights)
    num_long = int(np.sum(total_weights > 0))
    num_short = int(np.sum(total_weights < 0))
    num_positions = num_long + num_short
    active_risk = compute_risk(active_weights, covariance_matrix)
    total_risk = compute_risk(total_weights, covariance_matrix)
    utilization = dollars_allocated / account_value

    return PortfolioMetrics(
        gross_exposure=gross_exposure,
        net_exposure=net_exposure,
        num_long=num_long,
        num_short=num_short,
        num_positions=num_positions,
        active_risk=active_risk,
        total_risk=total_risk,
        utilization=utilization,
        account_value=account_value,
        dollars_allocated=dollars_allocated,
    )


# def get_top_long_positions(
#     shares: dy.DataFrame[Shares],
#     prices: dy.DataFrame[Prices],
#     dollars: dy.DataFrame[Dollars],
#     weights: dy.DataFrame[Weights],
#     benchmark: dy.DataFrame[Weights],
#     account_value: float,
#     top_n: int = 10,
# ) -> pl.DataFrame:
#     # Join all data together
#     positions = (
#         shares.join(prices, on="ticker", how="left")
#         .join(dollars, on="ticker", how="left")
#         .join(weights, on="ticker", how="left")
#         .join(
#             benchmark.select("ticker", pl.col("weight").alias("weight_bmk")),
#             on="ticker",
#             how="left",
#         )
#         .with_columns(
#             pl.col("weight_bmk").fill_null(0),
#         )
#         .with_columns(pl.col("weight").sub(pl.col("weight_bmk")).alias("weight_act"))
#         .with_columns(
#             pl.when(pl.col("weight_bmk") != 0)
#             .then((pl.col("weight_act") / pl.col("weight_bmk")) * 100)
#             .otherwise(None)
#             .alias("pct_chg_bmk")
#         )
#         .filter(pl.col("dollars") > 0)  # Only long positions
#         .sort("dollars", descending=True)
#         .head(top_n)
#         .select(
#             "ticker",
#             "shares",
#             "price",
#             "dollars",
#             "weight",
#             "weight_bmk",
#             "weight_act",
#             "pct_chg_bmk",
#         )
#     )

#     return positions


def get_top_long_positions(
    shares: dy.DataFrame[Shares],
    prices: dy.DataFrame[Prices],
    dollars: dy.DataFrame[Dollars],
    weights: dy.DataFrame[Weights],
    benchmark: dy.DataFrame[Weights],
    covariance_matrix: np.ndarray,
    account_value: float,
    top_n: int = 10,
) -> pl.DataFrame:
    # Build ticker-aligned active weight vector
    decomposed = (
        benchmark.select("ticker", pl.col("weight").alias("weight_bmk"))
        .join(weights.select("ticker", "weight"), on="ticker", how="left")
        .with_columns(pl.col("weight").fill_null(0.0))
        .with_columns((pl.col("weight") - pl.col("weight_bmk")).alias("weight_act"))
        .sort("ticker")
    )

    tickers = decomposed["ticker"].to_list()
    w_act = decomposed["weight_act"].to_numpy()

    mcar, tcar, pcar = compute_mcar_pcar(w_act, covariance_matrix)

    risk_attrib = pl.DataFrame(
        {
            "ticker": tickers,
            # marginal contribution to active risk
            "mcar": mcar,
            # total contribution to active risk
            "tcar": tcar,
            # percent contribution to active risk
            "pcar": pcar,
        }
    )

    # Join all position data
    positions = (
        shares.join(prices, on="ticker", how="left")
        .join(dollars, on="ticker", how="left")
        .join(weights, on="ticker", how="left")
        .join(
            benchmark.select("ticker", pl.col("weight").alias("weight_bmk")),
            on="ticker",
            how="left",
        )
        .with_columns(pl.col("weight_bmk").fill_null(0.0))
        .with_columns((pl.col("weight") - pl.col("weight_bmk")).alias("weight_act"))
        .with_columns(
            pl.when(pl.col("weight_bmk") != 0)
            .then((pl.col("weight_act") / pl.col("weight_bmk")) * 100)
            .otherwise(None)
            .alias("pct_chg_bmk")
        )
        .join(risk_attrib, on="ticker", how="left")
        .filter(pl.col("dollars") > 0)  # Only long positions
        .sort("dollars", descending=True)
        .head(top_n)
        .select(
            "ticker",
            "shares",
            "price",
            "dollars",
            "weight",
            "weight_bmk",
            "weight_act",
            "pct_chg_bmk",
            "mcar",
            "tcar",
            "pcar",
        )
    )

    return positions