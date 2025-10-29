import click
from pathlib import Path

import sf_trader.portfolio
from sf_trader.config import Config


@click.group()
def cli():
    """sf-trader: Interactive terminal trading application"""
    pass


@cli.command()
@click.option(
    "--config-path",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    default="config.yml",
    help="Path to configuration file",
)
@click.option(
    "--output-file-path",
    "-o",
    type=click.Path(exists=False, path_type=Path),
    default="portfolio.csv",
    help="Path to portfolio file",
)
def get_portfolio(config_path: Path, output_file_path: Path):
    config = Config(config_path)
    portfolio = sf_trader.portfolio.get_portfolio(config)
    portfolio.write_csv(output_file_path)


# @cli.command()
# @click.option(
#     "--config",
#     "-c",
#     type=click.Path(exists=True, path_type=Path),
#     default="config.yml",
#     help="Path to configuration file",
# )
# @click.option(
#     "--output-file-path",
#     "-o",
#     type=click.Path(exists=True, path_type=Path),
#     default="orders.csv",
#     help="Path to orders file",
# )
# def get_orders(config: Path, output_file_path: Path):
#     pass

# @cli.command()
# @click.option(
#     "--orders",
#     type=click.Path(exists=True, path_type=Path),
#     default="orderes.csv",
#     help="Path to orders file.",
# )
# def post_orders(orders: Path):
#     pass

if __name__ == "__main__":
    cli()
