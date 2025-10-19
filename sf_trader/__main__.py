import click
from pathlib import Path
from sf_trader.config import load_config
from rich import print

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
def main(config, dry_run):
    """SF-Trader: Interactive terminal trading application"""
    
    cfg = load_config(config)
    print(cfg)

if __name__ == '__main__':
    main()