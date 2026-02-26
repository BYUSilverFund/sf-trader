from sf_trader.components.models import Orders, Shares
from sf_trader.config import Config
import dataframely as dy
import sf_trader.utils.data
import sf_trader.utils.functions

from sf_trader.dal.dao.portfolio_dao import PortfolioDAO


def get_orders(
    optimal_shares: dy.DataFrame[Shares], config: Config
) -> dy.DataFrame[Orders]:
    # Connect to broker
    broker = config.broker
    port_dao = PortfolioDAO()

    # Config data loader
    sf_trader.utils.data.set_config(config=config)
    sf_trader.utils.functions.set_config(config=config)

    # Get current shares
    current_shares = broker.get_positions()

    # Compute ticker list
    tickers = list(
        set(current_shares["ticker"].to_list() + optimal_shares["ticker"].to_list())
    )

    # Get live prices
    prices = port_dao.get_prices_by_date(date=config.data_date, tickers=tickers)
    # TODO: Change to live price?

    # Get order deltas
    orders = sf_trader.utils.functions.get_order_deltas(
        current_shares=current_shares, optimal_shares=optimal_shares, prices=prices
    )

    # Disconnect from broker
    del broker
    del config.broker
    del port_dao

    return Orders.validate(orders)


def post_orders(orders: dy.DataFrame[Orders], config: Config) -> None:
    # Connect to broker
    broker = config.broker

    # Execute trades
    broker.post_orders(orders=orders)

    del broker
    del config.broker


def cancel_orders(config: Config) -> None:
    # Connect to broker
    broker = config.broker

    # Cancel all open orders
    broker.cancel_orders()

    del broker
    del config.broker
