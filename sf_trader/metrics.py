from dataclasses import dataclass

import dataframely as dy
import numpy as np
import polars as pl

from sf_trader.models import Assets, Orders, Portfolio, Positions


@dataclass(frozen=True)
class Benchmark:
    """Cap-weighted Russell 3000, with barrids sorted to match the covariance matrix."""

    tickers: list[str]
    weights: np.ndarray
    covariance_matrix: np.ndarray


@dataclass(frozen=True)
class PortfolioMetrics:
    active_risk: float
    total_risk: float
    gross_exposure: float
    net_exposure: float
    num_positions: int
    capital: float
    dollars_allocated: float


@dataclass(frozen=True)
class OrderMetrics:
    num_buys: int
    num_sells: int
    buy_value: float
    sell_value: float
    turnover: float
    skipped_no_quote: int
    skipped_small: int
    tc_current: float
    tc_current_risk_adjusted: float
    tc_after_orders: float
    tc_after_orders_risk_adjusted: float


def benchmark_weights(assets: dy.DataFrame[Assets], barrids: list[str]) -> pl.DataFrame:
    """Cap weights over the given barrids, sorted by barrid."""
    return (
        assets.filter(pl.col("barrid").is_in(barrids))
        .with_columns(
            pl.col("market_cap").truediv(pl.col("market_cap").sum()).alias("weight")
        )
        .select("barrid", "ticker", "weight")
        .sort("barrid")
    )


def align_weights(weights: pl.DataFrame, tickers: list[str]) -> np.ndarray:
    """Weights in benchmark order, with zero for names not held."""
    return (
        pl.DataFrame({"ticker": tickers}, schema={"ticker": pl.String})
        .join(weights.select("ticker", "weight"), on="ticker", how="left")["weight"]
        .fill_null(0.0)
        .to_numpy()
    )


def compute_risk(weights: np.ndarray, covariance_matrix: np.ndarray) -> float:
    return float(np.sqrt(weights @ covariance_matrix @ weights))


def get_portfolio_metrics(
    portfolio: dy.DataFrame[Portfolio], benchmark: Benchmark, capital: float
) -> PortfolioMetrics:
    weights = align_weights(portfolio, benchmark.tickers)
    gross = float(portfolio["weight"].abs().sum())

    return PortfolioMetrics(
        active_risk=compute_risk(weights - benchmark.weights, benchmark.covariance_matrix),
        total_risk=compute_risk(weights, benchmark.covariance_matrix),
        gross_exposure=gross,
        net_exposure=float(portfolio["weight"].sum()),
        num_positions=portfolio.height,
        capital=capital,
        dollars_allocated=gross * capital,
    )


def get_transfer_coefficient(
    optimal_active: np.ndarray,
    realized_active: np.ndarray,
    covariance_matrix: np.ndarray,
) -> tuple[float, float]:
    """Correlation of optimal vs realized active weights, plain and risk-adjusted."""
    tc = np.corrcoef(optimal_active, realized_active)[0, 1]

    # Risk-adjust by the Cholesky factor: w_adj = L' w, so w_adj' w_adj = w' S w
    chol = np.linalg.cholesky(covariance_matrix).T
    tc_risk_adj = np.corrcoef(chol @ optimal_active, chol @ realized_active)[0, 1]

    return float(tc), float(tc_risk_adj)


def holdings_after_orders(
    positions: dy.DataFrame[Positions], orders: dy.DataFrame[Orders]
) -> pl.DataFrame:
    """Share counts if every order fills."""
    signed = orders.select(
        "ticker",
        pl.when(pl.col("action").eq("BUY"))
        .then(pl.col("shares"))
        .otherwise(pl.col("shares").neg())
        .alias("delta"),
    )
    return (
        positions.join(signed, on="ticker", how="full", coalesce=True)
        .with_columns(pl.col("shares", "delta").fill_null(0.0))
        .select("ticker", pl.col("shares").add(pl.col("delta")).alias("shares"))
    )


def _weights_from_shares(
    shares: pl.DataFrame, prices: pl.DataFrame, capital: float
) -> pl.DataFrame:
    return shares.join(prices, on="ticker", how="inner").select(
        "ticker", pl.col("shares").mul("price").truediv(capital).alias("weight")
    )


def get_order_metrics(
    portfolio: dy.DataFrame[Portfolio],
    positions: dy.DataFrame[Positions],
    orders: dy.DataFrame[Orders],
    assets: dy.DataFrame[Assets],
    benchmark: Benchmark,
    capital: float,
    skipped_no_quote: int,
    skipped_small: int,
) -> OrderMetrics:
    prices = assets.select("ticker", "price").drop_nulls()

    def active(weights: pl.DataFrame) -> np.ndarray:
        return align_weights(weights, benchmark.tickers) - benchmark.weights

    optimal = active(portfolio)
    current = active(_weights_from_shares(positions, prices, capital))
    after = active(
        _weights_from_shares(holdings_after_orders(positions, orders), prices, capital)
    )

    covariance_matrix = benchmark.covariance_matrix
    tc_current, tc_current_adj = get_transfer_coefficient(optimal, current, covariance_matrix)
    tc_after, tc_after_adj = get_transfer_coefficient(optimal, after, covariance_matrix)

    buys = orders.filter(pl.col("action").eq("BUY"))
    sells = orders.filter(pl.col("action").eq("SELL"))
    buy_value = float(buys["value"].sum())
    sell_value = float(sells["value"].sum())

    return OrderMetrics(
        num_buys=buys.height,
        num_sells=sells.height,
        buy_value=buy_value,
        sell_value=sell_value,
        turnover=(buy_value + sell_value) / (2 * capital),
        skipped_no_quote=skipped_no_quote,
        skipped_small=skipped_small,
        tc_current=tc_current,
        tc_current_risk_adjusted=tc_current_adj,
        tc_after_orders=tc_after,
        tc_after_orders_risk_adjusted=tc_after_adj,
    )
