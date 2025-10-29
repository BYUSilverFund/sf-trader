import dataframely as dy
import polars as pl
import numpy as np
from sf_trader.components.models import Shares, Prices, Dollars, Weights
from sf_trader.config import Config
import sf_trader.utils.data
from rich.table import Table
from rich.console import Console
from rich.panel import Panel


def get_dollars(
    shares: dy.DataFrame[Shares], prices: dy.DataFrame[Prices]
) -> dy.DataFrame[Dollars]:
    dollars = (
        shares.join(prices, on="id", how="left")
        .with_columns(pl.col("shares").mul("price").alias("dollars"))
        .select("id", "dollars")
    )

    return Dollars.validate(dollars)


def get_weights_from_dollars(
    dollars: dy.DataFrame[Dollars], account_value: float
) -> dy.DataFrame[Weights]:
    weights = dollars.with_columns(
        (pl.col("dollars") / pl.lit(account_value)).alias("weight")
    ).sort("id")

    return Weights.validate(weights)


def get_re_keyed_weights(weights: dy.DataFrame[Weights]) -> dy.DataFrame[Weights]:
    # Get ticker to barrid mapping
    ticker_to_barrid = sf_trader.utils.data.get_ticker_barrid_mapping()
    weights = (
        weights.join(ticker_to_barrid, left_on="id", right_on="ticker")
        .select("barrid", "weight")
        .rename({"barrid": "id"})
        .sort("id")
    )
    return Weights.validate(weights)


def _merge_weights(
    benchmark: dy.DataFrame[Weights], weights: dy.DataFrame[Weights]
) -> pl.DataFrame:
    merge = (
        benchmark.select("id", pl.col("weight").alias("weight_bmk"))
        .join(
            weights,
            on="id",
            how="left",
        )
        .with_columns(
            pl.col("weight").fill_null(0),
        )
        .with_columns(pl.col("weight").sub(pl.col("weight_bmk")).alias("weight_act"))
        .with_columns(
            (pl.col("weight").truediv(pl.col("weight_bmk")).sub(1)).alias("pct_chg_bmk")
        )
        .sort("id")
    )

    return merge


def get_active_weights(
    benchmark: dy.DataFrame[Weights], weights: dy.DataFrame[Weights]
) -> dy.DataFrame[Weights]:
    merged_weights = _merge_weights(benchmark=benchmark, weights=weights)
    active_weights = merged_weights.select("id", "weight_act").rename(
        {"weight_act": "weight"}
    )
    return Weights.validate(active_weights)


def get_portfolio_summary(shares: dy.DataFrame[Shares], config: Config) -> None:
    # Connect to broker
    broker = config.broker

    # Configure modules
    sf_trader.utils.data.set_config(config=config)

    # Get account value
    account_value = broker.get_account_value()

    # Get tickers
    ids = shares["id"].to_list()

    # Get prices
    prices = sf_trader.utils.data.get_prices(tickers=ids)

    # Get dollars
    dollars = get_dollars(shares=shares, prices=prices)

    # Calculate portfolio weights from dollars
    weights = get_weights_from_dollars(dollars=dollars, account_value=account_value)

    # Re-key weights
    re_keyed_weights = get_re_keyed_weights(weights=weights)

    # Get benchmark weights
    benchmark = sf_trader.utils.data.get_benchmark_weights()

    # Get barrids
    barrids = benchmark["id"].to_list()

    # Get covariance matrix
    covariance_matrix = sf_trader.utils.data.get_covariance_matrix(barrids=barrids)

    # Decompose weights
    active_weights = get_active_weights(benchmark=benchmark, weights=re_keyed_weights)[
        "weight"
    ].to_numpy()
    total_weights = weights["weight"].to_numpy()

    # Compute metrics
    total_dollars_allocated = dollars["dollars"].sum()
    gross_exposure = np.sum(np.abs(total_weights))
    net_exposure = np.sum(total_weights)
    num_long = int(np.sum(total_weights > 0))
    num_short = int(np.sum(total_weights < 0))
    num_positions = num_long + num_short
    active_risk = compute_active_risk(active_weights, covariance_matrix)
    utilization = total_dollars_allocated / account_value

    # Create metrics table
    metrics_table = Table(show_header=False, box=None, padding=(0, 2))
    metrics_table.add_column("Metric", style="cyan")
    metrics_table.add_column("Value", style="bold white")

    metrics_table.add_row("Active Risk", f"{active_risk:.2%}")
    metrics_table.add_row("Gross Exposure", f"{gross_exposure:.2%}")
    metrics_table.add_row("Net Exposure", f"{net_exposure:.2%}")
    metrics_table.add_row("Total Positions", str(num_positions))
    metrics_table.add_row("- Long Positions", f"{num_long}")
    metrics_table.add_row("- Short Positions", f"{num_short}")
    metrics_table.add_row("Dollars Allocated", f"${total_dollars_allocated:,.0f}")
    metrics_table.add_row("Account Value", f"${account_value:,.0f}")
    metrics_table.add_row("Utilization", f"{utilization:.2%}")

    console = Console()

    console.print()
    console.print(
        Panel(
            metrics_table,
            title="[bold cyan]Portfolio Metrics[/bold cyan]",
            border_style="cyan",
        )
    )

    # # Top 10 Long Positions
    # long_positions = (
    #     merge.filter(pl.col("shares") > 0).sort("dollars", descending=True).head(10)
    # )
    # if len(long_positions) > 0:
    #     long_table = Table(
    #         title="Top 10 Long Positions", show_header=True, header_style="bold green"
    #     )
    #     long_table.add_column("Ticker", style="white")
    #     long_table.add_column("Shares", justify="right", style="white")
    #     long_table.add_column("Price", justify="right", style="white")
    #     long_table.add_column("Dollars", justify="right", style="green")
    #     long_table.add_column("Weight", justify="right", style="green")
    #     long_table.add_column("Active", justify="right", style="yellow")
    #     long_table.add_column("% Chg Bmk", justify="right", style="magenta")

    #     for row in long_positions.iter_rows(named=True):
    #         ticker = row.get("ticker", "N/A")
    #         shares_ = row["shares"]
    #         price = row["price"]
    #         dollars = row["dollars"]
    #         weight = row["weight"]
    #         active = row["weight_act"]
    #         pct_chg_bmk = row["pct_chg_bmk"]
    #         long_table.add_row(
    #             ticker,
    #             f"{shares_:,.0f}",
    #             f"${price:.2f}",
    #             f"${dollars:,.0f}",
    #             f"{weight:.2%}",
    #             f"{active:+.2%}",
    #             f"{pct_chg_bmk:+.2%}",
    #         )

    #     console.print()
    #     console.print(long_table)

    # # Top 10 Short Positions
    # short_positions = merge.filter(pl.col("shares") < 0).sort("dollars").head(10)
    # if len(short_positions) > 0:
    #     short_table = Table(
    #         title="Top 10 Short Positions", show_header=True, header_style="bold red"
    #     )
    #     short_table.add_column("Ticker", style="white")
    #     short_table.add_column("Shares", justify="right", style="white")
    #     short_table.add_column("Price", justify="right", style="white")
    #     short_table.add_column("Dollars", justify="right", style="red")
    #     short_table.add_column("Weight", justify="right", style="red")
    #     short_table.add_column("Active", justify="right", style="yellow")
    #     short_table.add_column("% Chg Bmk", justify="right", style="magenta")

    #     for row in short_positions.iter_rows(named=True):
    #         ticker = row.get("ticker", "N/A")
    #         shares_ = row["shares"]
    #         price = row["price"]
    #         dollars = row["dollars"]
    #         weight = row["weight"]
    #         active = row["weight_act"]
    #         pct_chg_bmk = row["pct_chg_bmk"]
    #         short_table.add_row(
    #             ticker,
    #             f"{shares_:,.0f}",
    #             f"${price:.2f}",
    #             f"${dollars:,.0f}",
    #             f"{weight:.2%}",
    #             f"{active:+.2%}",
    #             f"{pct_chg_bmk:+.2%}",
    #         )

    #     console.print()
    #     console.print(short_table)

    console.print()

    del broker
    del config.broker


def compute_active_risk(
    active_weights: np.ndarray, covariance_matrix: np.ndarray
) -> float:
    return np.sqrt(active_weights @ covariance_matrix @ active_weights.T)


# def create_portfolio_summary_from_shares(
#     shares: dy.DataFrame[Shares],
#     prices: dy.DataFrame[Prices],
#     trade_date: dt.date,
#     available_funds: float,
# ) -> dict:
#     # Convert trades to DataFrame
#     dollars = shares.join(other=prices, on="ticker", how="left").with_columns(
#         (pl.col("price") * pl.col("shares")).alias("dollars")
#     )

#     # Get tickers and barrids
#     ticker_to_barrid = get_ticker_barrid_mapping(trade_date=trade_date)

#     dollars_re_keyed = dollars.join(
#         pl.DataFrame(ticker_to_barrid), on="ticker", how="left"
#     )

#     # Calculate portfolio weights from dollar amounts
#     weights = dollars_re_keyed.with_columns(
#         (pl.col("dollars") / pl.lit(available_funds)).alias("weight")
#     ).sort("barrid")

#     # Get benchmark weights
#     benchmark = sfd.load_benchmark(start=trade_date, end=trade_date).sort("barrid")

#     barrids = benchmark["barrid"].to_list()

#     # Get covariance matrix
#     covariance_matrix = (
#         sfd.construct_covariance_matrix(date_=trade_date, barrids=barrids)
#         .drop("barrid")
#         .to_numpy()
#     )

#     merge = (
#         benchmark.select("barrid", pl.col("weight").alias("weight_bmk"))
#         .join(
#             weights,
#             on="barrid",
#             how="left",
#         )
#         .with_columns(
#             pl.col("weight").fill_null(0),
#         )
#         .with_columns(pl.col("weight").sub(pl.col("weight_bmk")).alias("weight_act"))
#         .with_columns(
#             (pl.col("weight").truediv(pl.col("weight_bmk")).sub(1)).alias("pct_chg_bmk")
#         )
#         .sort("barrid")
#     )

#     total_weights = merge["weight"].to_numpy()
#     active_weights = merge["weight_act"].to_numpy()

#     # Calculate metrics
#     total_dollars_allocated = merge["dollars"].sum()
#     gross_exposure = np.sum(np.abs(total_weights))
#     net_exposure = np.sum(total_weights)
#     num_long = int(np.sum(total_weights > 0))
#     num_short = int(np.sum(total_weights < 0))
#     num_positions = num_long + num_short
#     active_risk = compute_active_risk(active_weights, covariance_matrix)
#     utilization = (
#         total_dollars_allocated / available_funds if available_funds > 0 else 0
#     )

#     # Return metrics for programmatic access
#     return {
#         "active_risk": active_risk,
#         "gross_exposure": gross_exposure,
#         "net_exposure": net_exposure,
#         "num_positions": num_positions,
#         "num_long": num_long,
#         "num_short": num_short,
#         "total_dollars_allocated": total_dollars_allocated,
#         "available_funds": available_funds,
#         "utilization": utilization,
#         "trades_df": merge,
#     }
