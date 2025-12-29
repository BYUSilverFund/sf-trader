from components.models import Orders, Shares
from config import Config
import dataframely as dy
import utils.data
import utils.functions


def get_orders(
    optimal_shares: dy.DataFrame[Shares], config: Config
) -> dy.DataFrame[Orders]:
    # Connect to broker
    broker = config.broker

    # Config data loader
    utils.data.set_config(config=config)
    utils.functions.set_config(config=config)

    # Get current shares
    current_shares = broker.get_positions()

    # Compute ticker list
    tickers = list(
        set(current_shares["ticker"].to_list() + optimal_shares["ticker"].to_list())
    )

    # Get live prices
    prices = utils.data.get_prices(
        tickers=tickers
    )  # TODO: Change for live prices?

    # Get order deltas
    orders = utils.functions.get_order_deltas(
        current_shares=current_shares, optimal_shares=optimal_shares, prices=prices
    )

    # Disconnect from broker
    del broker
    del config.broker

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
