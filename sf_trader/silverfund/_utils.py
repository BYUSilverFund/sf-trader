import numpy as np
import polars as pl
import sf_quant.data as sfd
import datetime as dt

def compute_active_risk(
    active_weights: np.ndarray, covariance_matrix: np.ndarray
) -> float:
    return np.sqrt(active_weights @ covariance_matrix @ active_weights.T)


def create_portfolio_summary_with_weights(weights: pl.DataFrame, trade_date: dt.date):
    barrids = weights.sort("barrid")["barrid"].to_list()

    covariance_matrix = (
        sfd.construct_covariance_matrix(date_=trade_date, barrids=barrids)
        .drop("barrid")
        .to_numpy()
    )

    benchmark_weights = sfd.load_benchmark(start=trade_date, end=trade_date)

    weights_merge = (
        weights.join(
            benchmark_weights.select("barrid", "weight"),
            on="barrid",
            how="left",
            suffix="_bmk",
        )
        .with_columns(pl.col("weight").sub("weight_bmk").alias("weight_act"))
        .sort("barrid")
    )

    total_weights = weights_merge["weight"].to_numpy()
    active_weights = weights_merge["weight_act"].to_numpy()

    leverage = np.sum(total_weights)
    active_risk = compute_active_risk(active_weights, covariance_matrix)

    print()
    print("=" * 60)
    print("Portfolio Metrics")
    print("=" * 60)
    print(f"Active Risk: {active_risk:.2%}")
    print(f"Leverage: {leverage:.2%}")
    print()
    print("=" * 60)
    print("Top 10 Positions")
    print("=" * 60)
    print(weights_merge.sort("weight", descending=True).head(10))

def create_porfolio_summary_with_shares(shares: pl.DataFrame, total_funds: float) -> None:
    allocated_funds = shares['dollars_allocated'].sum()
    remaining_funds = total_funds - allocated_funds

    print()
    print("=" * 60)
    print("Allocation Summary")
    print("=" * 60)
    print(f"Total Funds: {total_funds:,.2f}")
    print(f"Allocated Funds: {allocated_funds:,.2f}")
    print(f"Remaining Funds: {remaining_funds:,.2f}")
    print()
    print("=" * 60)