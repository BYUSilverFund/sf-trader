import polars as pl
from components.models import PortfolioMetrics
from rich.table import Table


def generate_portfolio_metrics_table(metrics: PortfolioMetrics) -> Table:
    # Create metrics table
    table = Table(title="[bold cyan]Portfolio Metrics[/bold cyan]", padding=(0, 2))
    table.add_column("Metric", style="cyan", justify="left")
    table.add_column("Value", style="bold white", justify="right")

    table.add_row("Active Risk", f"{metrics.active_risk:.2%}")
    table.add_row("Total Risk", f"{metrics.total_risk:.2%}")
    table.add_row("Gross Exposure", f"{metrics.gross_exposure:.2%}")
    table.add_row("Net Exposure", f"{metrics.net_exposure:.2%}")
    table.add_row("Total Positions", str(metrics.num_positions))
    table.add_row("Long Positions", f"{metrics.num_long}")
    table.add_row("Short Positions", f"{metrics.num_short}")
    table.add_row("Dollars Allocated", f"${metrics.dollars_allocated:,.0f}")
    table.add_row("Account Value", f"${metrics.account_value:,.0f}")
    table.add_row("Utilization", f"{metrics.utilization:.2%}")

    return table


def generate_positions_table(
    positions: pl.DataFrame, title: str = "Top Long Positions"
) -> Table:
    table = Table(title=f"[bold cyan]{title}[/bold cyan]", padding=(0, 2))

    # Add columns
    table.add_column("Ticker", style="cyan", justify="left")
    table.add_column("Shares", style="white", justify="right")
    table.add_column("Price", style="white", justify="right")
    table.add_column("Dollars", style="bold white", justify="right")
    table.add_column("Weight", style="green", justify="right")
    table.add_column("Weight Bmk", style="yellow", justify="right")
    table.add_column("Weight Act", style="magenta", justify="right")
    table.add_column("% Chg Bmk", style="blue", justify="right")

    # Add rows
    for row in positions.iter_rows(named=True):
        pct_chg = (
            f"{row['pct_chg_bmk']:.1f}%" if row["pct_chg_bmk"] is not None else "N/A"
        )
        table.add_row(
            row["ticker"],
            f"{row['shares']:,.0f}",
            f"${row['price']:.2f}",
            f"${row['dollars']:,.0f}",
            f"{row['weight']:.2%}",
            f"{row['weight_bmk']:.2%}",
            f"{row['weight_act']:.2%}",
            pct_chg,
        )

    return table


def generate_orders_table(orders: pl.DataFrame, title: str = "Orders") -> Table:
    table = Table(title=f"[bold cyan]{title}[/bold cyan]", padding=(0, 2))

    # Add columns
    table.add_column("Ticker", style="cyan", justify="left")
    table.add_column("Shares", style="white", justify="right")
    table.add_column("Price", style="white", justify="right")
    table.add_column("Dollars", style="bold white", justify="right")
    table.add_column("To Trade", style="green", justify="right")
    table.add_column("Action", style="yellow", justify="right")

    # Add rows with conditional styling for action
    for row in orders.iter_rows(named=True):
        action = row["action"]
        # Color code the action
        if action == "BUY":
            action_styled = f"[green]{action}[/green]"
        elif action == "SELL":
            action_styled = f"[red]{action}[/red]"
        else:
            action_styled = f"[yellow]{action}[/yellow]"

        table.add_row(
            row["ticker"],
            f"{row['shares']:,.0f}",
            f"${row['price']:.2f}",
            f"${row['dollars']:,.0f}",
            f"{row['to_trade']:,.0f}",
            action_styled,
        )

    return table
