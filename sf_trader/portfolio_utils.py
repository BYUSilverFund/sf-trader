import polars as pl
from sf_trader.models import Config
import sf_quant.data as sfd
import sf_quant.optimizer as sfo
import datetime as dt
import numpy as np
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
import dataframely as dy
from sf_trader.models import Prices, AssetData, Alphas, Weights, Betas, Shares


def get_tradable_tickers(df: dy.DataFrame[Prices]) -> list[str]:
    return (
        df.filter(
            pl.col("price").ge(5),
        )["ticker"]
        .unique()
        .sort()
        .to_list()
    )


def get_alphas(
    asset_data: dy.DataFrame[AssetData], config: Config, trade_date: dt.date
) -> dy.DataFrame[Alphas]:
    signals = config.signals
    signal_combinator = config.signal_combinator
    ic = config.ic

    alphas = (
        asset_data.sort("barrid", "date")
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
        .filter(pl.col("date").eq(trade_date))
        .select("barrid", "alpha")
        .sort("barrid")
    )

    return Alphas.validate(alphas)


def get_ticker_barrid_mapping(trade_date: dt.date) -> pl.DataFrame:
    mapping = sfd.load_assets_by_date(
        date_=trade_date, columns=["ticker", "barrid"], in_universe=True
    )

    return mapping


def _compute_optimal_weights(
    barrids: list[str],
    alphas: dy.DataFrame[Alphas],
    betas: dy.DataFrame[Betas],
    config: Config,
    trade_date: dt.date,
) -> pl.DataFrame:
    barrids = sorted(barrids)
    alphas = alphas.sort("barrid")["alpha"].to_numpy()
    betas = betas.sort("barrid")["predicted_beta"].to_numpy()

    gamma = config.gamma
    constraints = config.constraints

    covariance_matrix = (
        sfd.construct_covariance_matrix(date_=trade_date, barrids=barrids)
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
    config: Config,
    trade_date: dt.date,
) -> dy.DataFrame[Weights]:
    decimal_places = config.decimal_places

    mapping = get_ticker_barrid_mapping(trade_date)
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
        config=config,
        trade_date=trade_date,
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
    weights: dy.DataFrame[Weights], prices: dy.DataFrame[Prices], available_funds: float
) -> dy.DataFrame[Shares]:
    optimal_shares = (
        weights.join(prices, on="ticker", how="left")
        .with_columns(pl.lit(available_funds).mul(pl.col("weight")).alias("dollars"))
        .with_columns(
            pl.col("dollars").truediv(pl.col("price")).floor().alias("shares")
        )
        .select(
            "ticker",
            "shares",
        )
    )

    return Shares.validate(optimal_shares)


def compute_active_risk(
    active_weights: np.ndarray, covariance_matrix: np.ndarray
) -> float:
    return np.sqrt(active_weights @ covariance_matrix @ active_weights.T)


def create_portfolio_summary_from_shares(
    shares: dy.DataFrame[Shares],
    prices: dy.DataFrame[Prices],
    trade_date: dt.date,
    available_funds: float,
) -> dict:
    # Convert trades to DataFrame
    dollars = shares.join(other=prices, on="ticker", how="left").with_columns(
        (pl.col("price") * pl.col("shares")).alias("dollars")
    )

    # Get tickers and barrids
    ticker_to_barrid = get_ticker_barrid_mapping(trade_date=trade_date)

    dollars_re_keyed = dollars.join(
        pl.DataFrame(ticker_to_barrid), on="ticker", how="left"
    )

    # Calculate portfolio weights from dollar amounts
    weights = dollars_re_keyed.with_columns(
        (pl.col("dollars") / pl.lit(available_funds)).alias("weight")
    ).sort("barrid")

    # Get benchmark weights
    benchmark = sfd.load_benchmark(start=trade_date, end=trade_date).sort("barrid")

    barrids = benchmark["barrid"].to_list()

    # Get covariance matrix
    covariance_matrix = (
        sfd.construct_covariance_matrix(date_=trade_date, barrids=barrids)
        .drop("barrid")
        .to_numpy()
    )

    merge = (
        benchmark.select("barrid", pl.col("weight").alias("weight_bmk"))
        .join(
            weights,
            on="barrid",
            how="left",
        )
        .with_columns(
            pl.col("weight").fill_null(0),
        )
        .with_columns(pl.col("weight").sub(pl.col("weight_bmk")).alias("weight_act"))
        .with_columns(
            (pl.col("weight").truediv(pl.col("weight_bmk")).sub(1)).alias("pct_chg_bmk")
        )
        .sort("barrid")
    )

    total_weights = merge["weight"].to_numpy()
    active_weights = merge["weight_act"].to_numpy()

    # Calculate metrics
    total_dollars_allocated = merge["dollars"].sum()
    gross_exposure = np.sum(np.abs(total_weights))
    net_exposure = np.sum(total_weights)
    num_long = int(np.sum(total_weights > 0))
    num_short = int(np.sum(total_weights < 0))
    num_positions = num_long + num_short
    active_risk = compute_active_risk(active_weights, covariance_matrix)
    utilization = (
        total_dollars_allocated / available_funds if available_funds > 0 else 0
    )

    # Return metrics for programmatic access
    return {
        "active_risk": active_risk,
        "gross_exposure": gross_exposure,
        "net_exposure": net_exposure,
        "num_positions": num_positions,
        "num_long": num_long,
        "num_short": num_short,
        "total_dollars_allocated": total_dollars_allocated,
        "available_funds": available_funds,
        "utilization": utilization,
        "trades_df": merge,
    }


def print_portfolio_summary(metrics: dict, console: Console | None = None) -> None:
    """Print portfolio metrics summary with detailed position tables."""
    if console is None:
        console = Console()

    merge = metrics["trades_df"]
    active_risk = metrics["active_risk"]
    gross_exposure = metrics["gross_exposure"]
    net_exposure = metrics["net_exposure"]
    num_positions = metrics["num_positions"]
    num_long = metrics["num_long"]
    num_short = metrics["num_short"]
    total_dollars_allocated = metrics["total_dollars_allocated"]
    available_funds = metrics["available_funds"]
    utilization = metrics["utilization"]

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
    metrics_table.add_row("Available Funds", f"${available_funds:,.0f}")
    metrics_table.add_row("Utilization", f"{utilization:.2%}")

    console.print()
    console.print(
        Panel(
            metrics_table,
            title="[bold cyan]Portfolio Metrics[/bold cyan]",
            border_style="cyan",
        )
    )

    # Top 10 Long Positions
    long_positions = (
        merge.filter(pl.col("shares") > 0).sort("dollars", descending=True).head(10)
    )
    if len(long_positions) > 0:
        long_table = Table(
            title="Top 10 Long Positions", show_header=True, header_style="bold green"
        )
        long_table.add_column("Ticker", style="white")
        long_table.add_column("Shares", justify="right", style="white")
        long_table.add_column("Price", justify="right", style="white")
        long_table.add_column("Dollars", justify="right", style="green")
        long_table.add_column("Weight", justify="right", style="green")
        long_table.add_column("Active", justify="right", style="yellow")
        long_table.add_column("% Chg Bmk", justify="right", style="magenta")

        for row in long_positions.iter_rows(named=True):
            ticker = row.get("ticker", "N/A")
            shares_ = row["shares"]
            price = row["price"]
            dollars = row["dollars"]
            weight = row["weight"]
            active = row["weight_act"]
            pct_chg_bmk = row["pct_chg_bmk"]
            long_table.add_row(
                ticker,
                f"{shares_:,.0f}",
                f"${price:.2f}",
                f"${dollars:,.0f}",
                f"{weight:.2%}",
                f"{active:+.2%}",
                f"{pct_chg_bmk:+.2%}",
            )

        console.print()
        console.print(long_table)

    # Top 10 Short Positions
    short_positions = merge.filter(pl.col("shares") < 0).sort("dollars").head(10)
    if len(short_positions) > 0:
        short_table = Table(
            title="Top 10 Short Positions", show_header=True, header_style="bold red"
        )
        short_table.add_column("Ticker", style="white")
        short_table.add_column("Shares", justify="right", style="white")
        short_table.add_column("Price", justify="right", style="white")
        short_table.add_column("Dollars", justify="right", style="red")
        short_table.add_column("Weight", justify="right", style="red")
        short_table.add_column("Active", justify="right", style="yellow")
        short_table.add_column("% Chg Bmk", justify="right", style="magenta")

        for row in short_positions.iter_rows(named=True):
            ticker = row.get("ticker", "N/A")
            shares_ = row["shares"]
            price = row["price"]
            dollars = row["dollars"]
            weight = row["weight"]
            active = row["weight_act"]
            pct_chg_bmk = row["pct_chg_bmk"]
            short_table.add_row(
                ticker,
                f"{shares_:,.0f}",
                f"${price:.2f}",
                f"${dollars:,.0f}",
                f"{weight:.2%}",
                f"{active:+.2%}",
                f"{pct_chg_bmk:+.2%}",
            )

        console.print()
        console.print(short_table)

    console.print()
