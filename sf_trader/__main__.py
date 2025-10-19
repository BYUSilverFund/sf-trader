# sf_trader/cli.py
import click
import sys
import logging
from pathlib import Path
from rich.console import Console
from rich.logging import RichHandler

# from .config import load_config, ConfigError
# from .trader import TradingSession

console = Console()

# Set up logging with rich for beautiful output
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(console=console, rich_tracebacks=True)]
)
logger = logging.getLogger("sf-trader")


@click.command()
@click.option(
    '--config',
    '-c',
    type=click.Path(exists=True, path_type=Path),
    default='config.yml',
    help='Path to configuration file'
)
@click.option(
    '--dry-run',
    is_flag=True,
    help='Simulate trades without executing'
)
@click.option(
    '--log-level',
    type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR'], case_sensitive=False),
    default='INFO',
    help='Set logging level'
)
@click.option(
    '--log-file',
    type=click.Path(path_type=Path),
    help='Write logs to file in addition to console'
)
def main(config, dry_run, log_level, log_file):
    """SF-Trader: Interactive terminal trading application"""
    
    # Set up logging
    logger.setLevel(log_level)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )
        logger.addHandler(file_handler)
    
    # Print header
    console.print("\n[bold cyan]SF-Trader[/bold cyan] - Trading Terminal", style="bold")
    console.print("=" * 50)
    
    if dry_run:
        console.print("[yellow]⚠️  DRY RUN MODE - No real trades will be executed[/yellow]\n")
    
    # try:
    #     # Load configuration
    #     console.print(f"Loading configuration from: [cyan]{config}[/cyan]")
    #     cfg = load_config(config)
        
    #     # Initialize trading session
    #     session = TradingSession(cfg, dry_run=dry_run)
        
    #     # Start interactive session
    #     session.run()
        
    # except ConfigError as e:
    #     console.print(f"[red]Configuration error:[/red] {e}", style="bold")
    #     sys.exit(1)
    # except KeyboardInterrupt:
    #     console.print("\n\n[yellow]Trading session interrupted by user[/yellow]")
    #     sys.exit(0)
    # except Exception as e:
    #     logger.exception("Unexpected error occurred")
    #     console.print(f"\n[red]Fatal error:[/red] {e}", style="bold")
    #     sys.exit(1)
    # finally:
    #     console.print("\n[dim]Session ended[/dim]\n")


if __name__ == '__main__':
    main()