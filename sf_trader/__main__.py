import click
from pathlib import Path
from rich.console import Console
import datetime as dt
import sf_trader.data_utils as du
import sf_trader.portfolio_utils as pu
import sf_trader.config_utils as cu
import sf_trader.trading_utils as tu
from typing import Callable, Any

console = Console()


@click.group()
def cli():
    """sf-trader: Interactive terminal trading application"""
    pass


def execute_step(
    step_name: str,
    func: Callable,
    *args,
    success_formatter: Callable[[Any], str] | None = None,
    pass_status: bool = False,
    **kwargs,
) -> Any:
    """Execute a step with a spinner and show success/failure status.

    Args:
        step_name: Name of the step to display
        func: Function to execute
        success_formatter: Optional function to format success message with result
        pass_status: If True, pass the status object to func as 'status' kwarg
        *args, **kwargs: Arguments to pass to func
    """
    with console.status(f"[bold blue]{step_name}...", spinner="dots") as status:
        try:
            if pass_status:
                kwargs["status"] = status
            result = func(*args, **kwargs)
            if success_formatter:
                success_msg = success_formatter(result)
                console.print(f"[green]✓[/green] {success_msg}")
            else:
                console.print(f"[green]✓[/green] {step_name}")
            return result
        except Exception as e:
            console.print(f"[red]✗[/red] {step_name}")
            console.print(f"[red]  Error: {str(e)}[/red]")
            raise


@cli.command()
@click.option(
    "--config",
    type=click.Path(exists=True, path_type=Path),
    default="config.yml",
    help="Path to configuration file",
)
@click.option("--dry-run", is_flag=True, help="Simulate trades without executing")
@click.option(
    "--prices",
    type=click.Choice(["ibkr", "barra"], case_sensitive=False),
    default="ibkr",
    help="Price source to use (default: ibkr)",
)
@click.option(
    "--trade-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="Trade date in YYYY-MM-DD format (default: today)",
)
def run(config: Path, dry_run: bool, prices: str, trade_date: dt.datetime | None):
    """Run the full trading pipeline"""
    console.print(
        "\n[bold cyan]sf-trader[/bold cyan] - Interactive Trading Application\n"
    )

    if trade_date is None:
        trade_date = dt.date.today()
    else:
        trade_date = trade_date.date()

    # 1. Parse config
    cfg = execute_step("Parsing configuration", cu.load_config, config)
    cu.print_config(cfg, console=console)

    # 2. Get universe
    tickers = execute_step(
        "Loading trading universe",
        du.get_tickers,
        trade_date=trade_date,
        success_formatter=lambda result: f"Loading trading universe ([cyan]{len(result):,}[/cyan] tickers)",
    )

    # 3. Get account value
    available_funds = execute_step(
        "Fetching available funds",
        du.get_available_funds,
        success_formatter=lambda result: f"Fetching available funds ([cyan]${result:,.0f}[/cyan])",
    )

    # 4. Get prices
    if prices == "barra":
        prices = execute_step(
            "Loading historical prices from Barra",
            du.get_barra_prices,
            trade_date=trade_date,
        )
    else:  # ibkr
        prices = execute_step(
            "Fetching prices from IBKR",
            du.get_ibkr_prices,
            tickers=tickers,
            pass_status=True,
        )

    # 5. Get tradable universe
    tradable_tickers = execute_step(
        "Filtering tradable tickers",
        pu.get_tradable_tickers,
        prices,
        success_formatter=lambda result: f"Filtering tradable tickers ([cyan]{len(result):,}[/cyan] tradable tickers)",
    )

    # 6. Get asset data
    lookback_days = max([signal.lookback_days for signal in cfg.signals])
    assets = execute_step(
        f"Loading asset data ({lookback_days} days lookback)",
        du.get_asset_data,
        tickers=tradable_tickers,
        trade_date=trade_date,
        lookback_days=lookback_days,
    )

    # 7. Get alphas
    alphas = execute_step(
        "Calculating alpha signals",
        pu.get_alphas,
        assets,
        config=cfg,
        trade_date=trade_date,
    )

    # 8. Get betas
    betas = execute_step(
        "Getting predicted betas",
        du.get_betas,
        tickers=tradable_tickers,
        trade_date=trade_date,
    )

    # 9. Get optimal weights
    optimal_weights = execute_step(
        "Optimizing portfolio weights",
        pu.get_optimal_weights,
        tickers=tradable_tickers,
        alphas=alphas,
        betas=betas,
        config=cfg,
        trade_date=trade_date,
    )

    # 10. Get optimal shares
    optimal_shares = execute_step(
        "Generating optimal shares",
        pu.get_optimal_shares,
        weights=optimal_weights,
        prices=prices,
        available_funds=available_funds,
    )

    # 11. Check portfolio metrics
    metrics = execute_step(
        "Computing portfolio metrics",
        pu.create_portfolio_summary_from_shares,
        shares=optimal_shares,
        prices=prices,
        trade_date=trade_date,
        available_funds=available_funds,
    )
    pu.print_portfolio_summary(metrics, console=console)

    # 12. Get current positions (shares)
    current_shares = execute_step("Fetching positions from IBKR", du.get_ibkr_positions)

    # 13. Compute trades
    trades = execute_step(
        "Computing trade list",
        tu.compute_orders,
        current_shares=current_shares,
        optimal_shares=optimal_shares,
        prices=prices,
        config=cfg,
    )

    # 14. Get top long positions
    top_long_positions = execute_step(
        "Computing top long positions",
        tu.get_top_long_positions,
        trades=trades,
        current_shares=current_shares,
        optimal_shares=optimal_shares,
    )
    tu.print_top_long_positions(top_long_positions, console=console)

    # 15. Execute trades
    if not dry_run:
        console.print("\n[bold green]Portfolio ready for execution![/bold green]\n")
        execute_step(
            "Executing trades",
            tu.submit_limit_orders,
            trades=trades,
        )


@cli.command()
def clear_orders():
    """Cancel all open orders in IBKR"""
    console.print("\n[bold cyan]sf-trader[/bold cyan] - Clear Orders\n")

    result = execute_step(
        "Cancelling all open orders",
        tu.clear_ibkr_orders,
        success_formatter=lambda result: f"Cancelled {len(result)} order(s)",
    )

    if len(result) > 0:
        console.print("\n[bold]Order Cancellation Results:[/bold]")
        print(result)

    console.print("\n[bold green]Done![/bold green]\n")


if __name__ == "__main__":
    cli()
