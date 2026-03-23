import click
from pathlib import Path

from sf_trader.config import Config
from sf_trader.dal.dao.surface_dao import SurfaceDAO
from sf_trader.service.order_service import OrderService
from sf_trader.service.portfolio_service import PortfolioService
from sf_trader.service.summary_service import SummaryService


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
def get_portfolio(config_path: Path):
    config = Config(config_path)
    portfolio_service = PortfolioService(config)

    portfolio_service.get_write_portfolio()


@cli.command()
@click.option(
    "--config-path",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    default="config.yml",
    help="Path to configuration file",
)
def get_orders(config_path: Path):
    config = Config(config_path)
    order_service = OrderService(config=config)

    order_service.get_write_orders()


@cli.command()
@click.option(
    "--config-path",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    default="config.yml",
    help="Path to configuration file",
)
def get_portfolio_summary(config_path: Path):
    config = Config(config_path)
    surface_dao = SurfaceDAO(config)
    summary_service = SummaryService(config)

    portfolio = surface_dao.read_portfolio()
    summary_service.get_portfolio_summary(shares=portfolio)


@cli.command()
@click.option(
    "--config-path",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    default="config.yml",
    help="Path to configuration file",
)
def get_orders_summary(config_path: Path):
    config = Config(config_path)
    surface_dao = SurfaceDAO(config)
    summary_service = SummaryService(config)

    orders = surface_dao.read_orders()
    portfolio = surface_dao.read_portfolio()
    summary_service.get_orders_summary(shares=portfolio, orders=orders)


@cli.command()
@click.option(
    "--config-path",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    default="config.yml",
    help="Path to configuration file",
)
def post_orders(config_path: Path):
    config = Config(config_path)
    order_service = OrderService(config)

    order_service.post_orders()


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
    order_service = OrderService(config=config)

    order_service.cancel_orders()


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
