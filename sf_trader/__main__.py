import click
from pathlib import Path
from rich.console import Console
import datetime as dt
import sf_trader.data_utils as du
import sf_trader.portfolio_utils as pu
import sf_trader.config_utils as cu
import sf_trader.trading_utils as tu
from typing import Callable, Any
import polars as pl

console = Console()

def execute_step(step_name: str, func: Callable, *args, success_formatter: Callable[[Any], str] | None = None, pass_status: bool = False, **kwargs) -> Any:
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
                kwargs['status'] = status
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

@click.command()
@click.option(
    '--config',
    type=click.Path(exists=True, path_type=Path),
    default='config.yml',
    help='Path to configuration file'
)
@click.option(
    '--dry-run',
    is_flag=True,
    help='Simulate trades without executing'
)
def main(config: Path, dry_run: bool):
    """sf-trader: Interactive terminal trading application"""
    console.print("\n[bold cyan]sf-trader[/bold cyan] - Interactive Trading Application\n")

    trade_date = dt.date(2025, 10, 17)

    # 1. Parse config
    cfg = execute_step(
        "Parsing configuration",
        cu.load_config,
        config
    )
    cu.print_config(cfg, console=console)

    # 2. Get universe
    tickers = execute_step(
        "Loading trading universe",
        du.get_tickers,
        trade_date=trade_date,
        config=cfg,
        success_formatter=lambda result: f"Loading trading universe ([cyan]{len(result):,}[/cyan] tickers)"
    )

    # 3. Get account value
    available_funds = execute_step(
        "Fetching available funds",
        du.get_available_funds,
        success_formatter=lambda result: f"Fetching available funds ([cyan]${result:,.0f}[/cyan])"
    )

    # 4. Get prices
    if dry_run:
        prices = execute_step(
            "Loading historical prices",
            du.get_barra_prices,
            trade_date=trade_date
        )
    else:
        prices = execute_step(
            "Fetching prices from IBKR",
            du.get_ibkr_prices,
            tickers=tickers,
            pass_status=True
        )

    # 5. Get tradable universe
    tradable_tickers = execute_step(
        "Filtering tradable tickers",
        pu.get_tradable_tickers,
        prices,
        success_formatter=lambda result: f"Filtering tradable tickers ([cyan]{len(result):,}[/cyan] tradable tickers)"
    )

    # 6. Get data
    lookback_days = max([signal.lookback_days for signal in cfg.signals])
    assets = execute_step(
        f"Loading asset data ({lookback_days} days lookback)",
        du.get_asset_data,
        tickers=tradable_tickers,
        trade_date=trade_date,
        lookback_days=lookback_days
    )

    # 7. Get alphas
    alphas = execute_step(
        "Calculating alpha signals",
        pu.get_alphas,
        assets,
        config=cfg,
        trade_date=trade_date
    )

    # 8. Get portfolio weights
    weights = execute_step(
        "Optimizing portfolio weights",
        pu.get_portfolio_weights,
        alphas,
        config=cfg,
        trade_date=trade_date
    )

    # 9. Get trades
    trades = execute_step(
        "Generating trade orders",
        pu.get_trades,
        weights,
        prices,
        config=cfg,
        available_funds=available_funds
    )

    # 10. Check portfolio metrics
    execute_step(
        "Computing portfolio metrics",
        pu.create_portfolio_summary_with_trades,
        trades,
        trade_date,
        available_funds
    )

    # if not dry_run:
    #     # 11. Execute trades
    #     console.print("\n[bold green]Portfolio ready for execution![/bold green]\n")
    #     result = execute_step(
    #         "Executing trades",
    #         tu.submit_limit_orders,
    #         trades=trades,
    #     )
    
if __name__ == '__main__':
    main()