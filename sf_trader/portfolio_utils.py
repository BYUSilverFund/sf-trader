import polars as pl
from sf_trader.models import Config
import sf_quant.data as sfd
import sf_quant.optimizer as sfo
import datetime as dt
import numpy as np
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

def get_tradable_tickers(df: pl.DataFrame) -> list[str]:
    return (
        df
        .filter(
            pl.col('price').ge(5)
        )
        ['ticker']
        .unique()
        .sort()
        .to_list()
    )

def get_trades(weights: pl.DataFrame, prices: pl.DataFrame, config: Config, available_funds: float) -> list[dict]:
    return (
        weights
        .join(
            prices,
            on='ticker',
            how='left'
        )
        .with_columns(
            pl.lit(available_funds).mul(pl.col('weight')).alias('dollars')
        )
        .with_columns(
            pl.col('dollars').truediv(pl.col('price')).floor().alias('shares')
        )
        .select(
            'ticker',
            'price',
            'shares'
        )
        .to_dicts()
    )
def get_alphas(df: pl.DataFrame, config: Config, trade_date: dt.date) -> pl.DataFrame:
    signals = config.signals
    signal_combinator = config.signal_combinator
    ic = config.ic

    return (
        df
        .sort('barrid', 'date')
        # Compute signals
        .with_columns([signal.expr for signal in signals])
        # Compute scores
        .with_columns([
            pl.col(signal.name).sub(pl.col(signal.name).mean()).truediv(pl.col(signal.name).std())
            for signal in signals
        ])
        # Compute alphas
        .with_columns([
            pl.col(signal.name).mul(pl.lit(ic)).mul(pl.col('specific_risk'))
            for signal in signals
        ])
        # Fill null alphas with 0
        .with_columns(
            pl.col(signal.name).fill_null(0)
            for signal in signals    
        )
        # Combine alphas
        .with_columns(
            signal_combinator.combine_fn([signal.name for signal in signals])
        )
        # Get trade date
        .filter(
            pl.col('date')
            .eq(trade_date)
        )
        .select(
            'date',
            'barrid',
            'ticker',
            'predicted_beta',
            'alpha'
        )
        .sort('barrid', 'date')
    )

def get_ticker_barrid_mapping(trade_date: dt.date) -> list[str]:
    return (
        sfd.load_assets_by_date(
            date_=trade_date,
            columns=['ticker', 'barrid'],
            in_universe=True
        )
    )

def get_portfolio_weights(df: pl.DataFrame, config: Config, trade_date: dt.date) -> pl.DataFrame:
    df = df.sort('barrid')

    gamma = config.gamma
    decimal_places = config.decimal_places

    barrids = df['barrid'].to_list()
    alphas = df['alpha'].to_list()
    betas = df['predicted_beta'].to_list()

    covariance_matrix = (
        sfd.construct_covariance_matrix(
            date_=trade_date,
            barrids=barrids
        )
        .drop('barrid')
        .to_numpy()
    )

    constraints = [
        sfo.FullInvestment(),
        sfo.NoBuyingOnMargin(),
        sfo.LongOnly(),
        sfo.UnitBeta()
    ]

    weights = sfo.mve_optimizer(
        ids=barrids,
        alphas=alphas,
        betas=betas,
        covariance_matrix=covariance_matrix,
        gamma=gamma,
        constraints=constraints
    )

    return (
        weights
        .with_columns(pl.col("weight").round(4))
        .filter(pl.col("weight").ge(1 * 10**-decimal_places))
        .join(
            other=df.select('barrid', 'ticker'),
            on='barrid',
            how='left'
        )
        .sort('barrid', 'weight')
    )

def compute_active_risk(
    active_weights: np.ndarray, covariance_matrix: np.ndarray
) -> float:
    return np.sqrt(active_weights @ covariance_matrix @ active_weights.T)

def create_portfolio_summary_with_trades(trades: list[dict], trade_date: dt.date, available_funds: float):
    """
    Create and display a comprehensive portfolio summary from a list of trades.

    Args:
        trades: List of dicts with keys: ticker, price, shares
        trade_date: Date for benchmark and covariance matrix
        available_funds: Total available funds for the portfolio

    Returns a dictionary with portfolio metrics for programmatic access if needed.
    """
    console = Console()

    # Convert trades to DataFrame
    trades_df = pl.DataFrame(trades).with_columns(
        (pl.col("price") * pl.col("shares")).alias("dollars")
    )

    # Get tickers and barrids
    ticker_to_barrid = get_ticker_barrid_mapping(trade_date=trade_date)

    trades_df = trades_df.join(
        pl.DataFrame(ticker_to_barrid),
        on="ticker",
        how="left"
    )

    # Calculate portfolio weights from dollar amounts
    trades_df = trades_df.with_columns(
        (pl.col("dollars") / pl.lit(available_funds)).alias("weight")
    ).sort("barrid")

    # Get benchmark weights
    benchmark = sfd.load_benchmark(start=trade_date, end=trade_date).sort('barrid')

    barrids = benchmark["barrid"].to_list()

    # Get covariance matrix
    covariance_matrix = (
        sfd.construct_covariance_matrix(date_=trade_date, barrids=barrids)
        .drop("barrid")
        .to_numpy()
    )

    trades_merge = (
        benchmark
        .select('barrid', pl.col('weight').alias('weight_bmk'))
        .join(
            trades_df,
            on="barrid",
            how="left",
        )
        .with_columns(
            pl.col("weight").fill_null(0),
        )
        .with_columns(
            pl.col("weight").sub(pl.col("weight_bmk")).alias("weight_act")
        )
        .sort("barrid")
    )

    total_weights = trades_merge["weight"].to_numpy()
    active_weights = trades_merge["weight_act"].to_numpy()

    # Calculate metrics
    total_dollars_allocated = trades_df["dollars"].sum()
    gross_exposure = np.sum(np.abs(total_weights))
    net_exposure = np.sum(total_weights)
    num_long = int(np.sum(total_weights > 0))
    num_short = int(np.sum(total_weights < 0))
    num_positions = num_long + num_short
    active_risk = compute_active_risk(active_weights, covariance_matrix)
    utilization = total_dollars_allocated / available_funds if available_funds > 0 else 0

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
    console.print(Panel(metrics_table, title="[bold cyan]Portfolio Metrics[/bold cyan]", border_style="cyan"))

    # Top 10 Long Positions
    long_positions = trades_merge.filter(pl.col("shares") > 0).sort("dollars", descending=True).head(10)
    if len(long_positions) > 0:
        long_table = Table(title="Top 10 Long Positions", show_header=True, header_style="bold green")
        long_table.add_column("Ticker", style="white")
        long_table.add_column("Shares", justify="right", style="white")
        long_table.add_column("Price", justify="right", style="white")
        long_table.add_column("Dollars", justify="right", style="green")
        long_table.add_column("Weight", justify="right", style="green")
        long_table.add_column("Active", justify="right", style="yellow")

        for row in long_positions.iter_rows(named=True):
            ticker = row.get("ticker", "N/A")
            shares = row["shares"]
            price = row["price"]
            dollars = row["dollars"]
            weight = row["weight"]
            active = row["weight_act"]
            long_table.add_row(
                ticker,
                f"{shares:,.0f}",
                f"${price:.2f}",
                f"${dollars:,.0f}",
                f"{weight:.2%}",
                f"{active:+.2%}"
            )

        console.print()
        console.print(long_table)

    # Top 10 Short Positions
    short_positions = trades_merge.filter(pl.col("shares") < 0).sort("dollars").head(10)
    if len(short_positions) > 0:
        short_table = Table(title="Top 10 Short Positions", show_header=True, header_style="bold red")
        short_table.add_column("Ticker", style="white")
        short_table.add_column("Shares", justify="right", style="white")
        short_table.add_column("Price", justify="right", style="white")
        short_table.add_column("Dollars", justify="right", style="red")
        short_table.add_column("Weight", justify="right", style="red")
        short_table.add_column("Active", justify="right", style="yellow")

        for row in short_positions.iter_rows(named=True):
            ticker = row.get("ticker", "N/A")
            shares = row["shares"]
            price = row["price"]
            dollars = row["dollars"]
            weight = row["weight"]
            active = row["weight_act"]
            short_table.add_row(
                ticker,
                f"{shares:,.0f}",
                f"${price:.2f}",
                f"${dollars:,.0f}",
                f"{weight:.2%}",
                f"{active:+.2%}"
            )

        console.print()
        console.print(short_table)

    console.print()

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
        "trades_df": trades_merge
    }
