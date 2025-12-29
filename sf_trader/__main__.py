import click
from pathlib import Path

import polars as pl

import portfolio
import orders
import portfolio_summary
import orders_summary
from config import Config
from components.models import Orders, Shares


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
    portfolio_ = portfolio.get_portfolio(config)
    portfolio_.write_csv(output_file_path)


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
    portfolio_ = Shares.validate(pl.read_csv(portfolio_path))
    orders_ = orders.get_orders(optimal_shares=portfolio_, config=config)
    orders_.write_csv(output_file_path)


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
    portfolio_ = Shares.validate(pl.read_csv(portfolio_path))
    portfolio_summary.get_portfolio_summary(shares=portfolio_, config=config)


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
    "--orders-path",
    "-o",
    type=click.Path(exists=True, path_type=Path),
    default="orders.csv",
    help="Path to orders file",
)
def get_orders_summary(config_path: Path, portfolio_path: Path, orders_path: Path):
    config = Config(config_path)
    orders_ = Orders.validate(pl.read_csv(orders_path))
    portfolio_ = Shares.validate(pl.read_csv(portfolio_path))
    orders_summary.get_orders_summary(
        shares=portfolio_, orders=orders_, config=config
    )


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
    "-o",
    type=click.Path(exists=True, path_type=Path),
    default="orders.csv",
    help="Path to orders file.",
)
def post_orders(config_path: Path, orders_path: Path):
    config = Config(config_path)
    orders_ = Orders.validate(pl.read_csv(orders_path))
    orders.post_orders(orders=orders_, config=config)


@cli.command()
@click.option(
    "--config-path",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    default="config.yml",
    help="Path to configuration file",
)
def cancel_orders(config_path: Path):
    config = Config(config_path)
    orders.cancel_orders(config=config)


@cli.command()
@click.option(
    "--config-path",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    default="config.yml",
    help="Path to configuration file",
)
def get_account_value(config_path: Path):
    """Get the current account value (net liquidation)"""
    config = Config(config_path)
    account_value = config.broker.get_account_value()
    print(f"Account Value: ${account_value:,.2f}")


if __name__ == "__main__":
    cli()
