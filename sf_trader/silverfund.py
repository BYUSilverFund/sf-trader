import polars as pl
import sf_quant.data as sfd
import sf_quant.optimizer as sfo
import datetime as dt
import os
import numpy as np

IC = 0.05

END = dt.date(2025, 9, 15)
START = END - dt.timedelta(days=365)

def get_tickers() -> list[str]:
    tickers_path = f"tickers_{END}.parquet"
    if not os.path.exists(tickers_path):
        (
            sfd.load_assets_by_date(
                date_=END,
                in_universe=True,
                columns=['ticker']
            )
            .write_parquet(tickers_path)
        )
    
    return pl.read_parquet(tickers_path)['ticker'].to_list()


def get_portfolio_weights(gamma: float = 2.0, decimal_places: int = 4) -> pl.DataFrame:

    assets_file_name = f"assets_{END}_{START}.parquet"

    if not os.path.exists(assets_file_name):
        print("Downloading assets data...")

        assets = sfd.load_assets(
            start=START,
            end=END,
            in_universe=True,
            columns=[
                "date",
                "barrid",
                "ticker",
                "price",
                "return",
                "specific_risk",
                "predicted_beta",
            ],
        )

        assets.write_parquet(assets_file_name)

    assets = pl.read_parquet(assets_file_name)

    signals = (
        assets.sort("barrid", "date")
        .with_columns(pl.col("return", "specific_risk").truediv(100))
        .with_columns(
            pl.col("return")
            .log1p()
            .rolling_sum(window_size=230)
            .shift(22)
            .alias("momentum")
        )
        .with_columns(
            pl.col("momentum")
            .sub(pl.col("momentum").mean())
            .truediv(pl.col("momentum").std())
            .alias("score")
        )
        .with_columns(pl.lit(IC).mul("score").mul("specific_risk").alias("alpha"))
        .filter(pl.col("date").eq(END), pl.col("alpha").is_not_null())
        .sort("barrid")
    )

    barrids = signals["barrid"].to_list()
    alphas = signals["alpha"].to_list()
    betas = signals["predicted_beta"].to_list()

    covariance_matrix = (
        sfd.construct_covariance_matrix(date_=END, barrids=barrids)
        .drop("barrid")
        .to_numpy()
    )

    constraints = [
        sfo.LongOnly(),
        sfo.NoBuyingOnMargin(),
        sfo.FullInvestment(),
        sfo.UnitBeta(),
    ]

    weights = sfo.mve_optimizer(
        ids=barrids,
        alphas=alphas,
        covariance_matrix=covariance_matrix,
        constraints=constraints,
        gamma=gamma,
        betas=betas,
    )

    return (
        weights.with_columns(pl.col("weight").round(4))
        .filter(pl.col("weight").ge(1 * 10**-decimal_places))
        .join(signals.select("barrid", "ticker"), on="barrid", how="left")
        .select("barrid", "ticker", "weight")
        .sort("weight", descending=True)
    )


def compute_active_risk(
    active_weights: np.ndarray, covariance_matrix: np.ndarray
) -> float:
    return np.sqrt(active_weights @ covariance_matrix @ active_weights.T)


def create_portfolio_summary(weights: pl.DataFrame):
    barrids = weights.sort("barrid")["barrid"].to_list()

    covariance_matrix = (
        sfd.construct_covariance_matrix(date_=END, barrids=barrids)
        .drop("barrid")
        .to_numpy()
    )

    benchmark_weights = sfd.load_benchmark(start=END, end=END)

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


if __name__ == "__main__":
    # ===== Parameters =====
    gamma = 50
    decimal_places = 4

    # ===== Weights =====
    weights = get_portfolio_weights(gamma=gamma, decimal_places=decimal_places)

    # ===== Summary =====
    create_portfolio_summary(weights=weights)
