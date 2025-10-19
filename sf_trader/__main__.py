import click
from pathlib import Path
from rich.console import Console
from rich.status import Status
from rich.table import Table
from rich.panel import Panel
import datetime as dt
import sf_trader.data_utils as du
import sf_trader.portfolio_utils as pu
import sf_trader.config_utils as cu
from sf_trader.models import Config
from typing import Callable, Any

console = Console()

def execute_step(step_name: str, func: Callable, *args, **kwargs) -> Any:
    """Execute a step with a spinner and show success/failure status."""
    with console.status(f"[bold blue]{step_name}...", spinner="dots"):
        try:
            result = func(*args, **kwargs)
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

    trade_date = dt.date(2025, 10, 16)

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
        trade_date=trade_date
    )

    # 3. Get account value
    available_funds = execute_step(
        "Fetching available funds",
        du.get_available_funds
    )

    # 4. Get prices
    if dry_run:
        prices = execute_step(
            "Loading historical prices",
            du.get_barra_prices,
            trade_date=trade_date
        )

    # 5. Get tradable universe
    tradable_tickers = execute_step(
        "Filtering tradable tickers",
        pu.get_tradable_tickers,
        prices
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
    portfolio_metrics = execute_step(
        "Computing portfolio metrics",
        pu.create_portfolio_summary_with_trades,
        trades,
        trade_date,
        available_funds
    )

    # # 11. Execute trades
    # console.print("\n[bold green]Portfolio ready for execution![/bold green]\n")
    
if __name__ == '__main__':
    main()