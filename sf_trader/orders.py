from sf_trader.config import Config
import sf_trader.domain.functions

from sf_trader.dal.dao.portfolio_dao import PortfolioDAO
from sf_trader.dal.models.schema_models import SharesDF, OrdersDF, OrdersSchema


def get_orders(
    optimal_shares: SharesDF, config: Config
) -> OrdersDF:
    # Connect to broker
    broker = config.broker
    port_dao = PortfolioDAO()

    # Config data loader
    sf_trader.utils.data.set_config(config=config)
    sf_trader.domain.functions.set_config(config=config)

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
    orders = sf_trader.domain.functions.get_order_deltas(
        current_shares=current_shares, optimal_shares=optimal_shares, prices=prices
    )

    # Disconnect from broker
    del broker
    del config.broker
    del port_dao

    return OrdersSchema.validate(orders)


def post_orders(orders: OrdersDF, config: Config) -> None:
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
