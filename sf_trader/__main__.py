import click
from pathlib import Path

import polars as pl

import sf_trader.portfolio
import sf_trader.orders
import sf_trader.portfolio_summary
from sf_trader.config import Config
from sf_trader.components.models import Orders, Shares


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


@cli.command()
@click.option(
    "--config-path",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    default="config.yml",
    help="Path to configuration file",
)
@click.option(
    "--portfolio-path",
    "-p",
    type=click.Path(exists=True, path_type=Path),
    default="portfolio.csv",
    help="Path to portfolio file",
)
@click.option(
    "--output-file-path",
    "-o",
    type=click.Path(exists=False, path_type=Path),
    default="orders.csv",
    help="Path to orders file",
)
def get_orders(config_path: Path, portfolio_path: Path, output_file_path: Path):
    config = Config(config_path)
    portfolio = Shares.validate(pl.read_csv(portfolio_path))
    orders = sf_trader.orders.get_orders(optimal_shares=portfolio, config=config)
    orders.write_csv(output_file_path)


@cli.command()
@click.option(
    "--config-path",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    default="config.yml",
    help="Path to configuration file",
)
@click.option(
    "--portfolio-path",
    "-p",
    type=click.Path(exists=True, path_type=Path),
    default="portfolio.csv",
    help="Path to portfolio file",
)
def get_portfolio_summary(config_path: Path, portfolio_path: Path):
    config = Config(config_path)
    portfolio = Shares.validate(pl.read_csv(portfolio_path))
    sf_trader.portfolio_summary.get_portfolio_summary(shares=portfolio, config=config)


@cli.command()
@click.option(
    "--config-path",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    default="config.yml",
    help="Path to configuration file",
)
@click.option(
    "--orders-path",
    type=click.Path(exists=True, path_type=Path),
    default="orders.csv",
    help="Path to orders file.",
)
def post_orders(config_path: Path, orders_path: Path):
    config = Config(config_path)
    orders = Orders.validate(pl.read_csv(orders_path))
    sf_trader.orders.post_orders(orders=orders, config=config)


if __name__ == "__main__":
    cli()
