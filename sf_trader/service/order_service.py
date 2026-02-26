from sf_trader.config import Config
import sf_trader.domain.computations as computations

from sf_trader.dal.dao.portfolio_dao import PortfolioDAO
from sf_trader.dal.models.schema_models import SharesDF, OrdersDF, OrdersSchema

class OrderService:
    def __init__(self, config: Config, portfolio_dao: PortfolioDAO):
        self.portfolio_dao = portfolio_dao
        self.config = config
        self.broker = config.broker


    def get_orders(self, optimal_shares: SharesDF) -> OrdersDF:

        # Config helper
        computations.set_config(config=self.config)

        # Get current shares
        current_shares = self.broker.get_positions()

        # Compute ticker list
        tickers = list(
            set(current_shares["ticker"].to_list() + optimal_shares["ticker"].to_list())
        )

        # Get live prices
        prices = self.portfolio_dao.get_prices_by_date(date=self.config.data_date, tickers=tickers)
        # TODO: Change to live price?

        # Get order deltas
        orders = computations.get_order_deltas(
            current_shares=current_shares, optimal_shares=optimal_shares, prices=prices
        )

        return OrdersSchema.validate(orders)


    def post_orders(self, orders: OrdersDF) -> None:
        # Connect to broker
        broker = self.broker

        # Execute trades
        broker.post_orders(orders=orders)


    def cancel_orders(self) -> None:
        # Connect to broker
        broker = self.broker

        # Cancel all open orders
        broker.cancel_orders()
