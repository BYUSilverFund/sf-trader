from sf_trader.config import Config
import sf_trader.domain.computations as computations

from sf_trader.dal.dao.portfolio_dao import PortfolioDAO
from sf_trader.dal.dao.surface_dao import SurfaceDAO
from sf_trader.dal.models.schema_models import OrdersDF, OrdersSchema


class OrderService:
    def __init__(self, config: Config, portfolio_dao: PortfolioDAO | None = None, surface_dao: SurfaceDAO | None = None):
        self.portfolio_dao = portfolio_dao or PortfolioDAO()
        self.surface_dao = surface_dao or SurfaceDAO(config)
        self.config = config
        self.broker = config.broker
    
    
    def get_write_orders(self) -> OrdersDF:
        """Reads optimal shares and computes orders, then writes orders to surface"""

        # Configure helper
        computations.set_config(config=self.config)

        # Get optimal shares (the csv saved portfolio)
        optimal_shares = self.surface_dao.read_portfolio()

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

        # Write orders to surface
        self.surface_dao.write_orders(OrdersSchema.validate(orders))

        return orders


    def post_orders(self) -> None:
        # Connect to broker
        broker = self.broker

        # Get orders from surface
        orders = self.surface_dao.read_orders()

        # Execute trades
        broker.post_orders(orders=orders)


    def cancel_orders(self) -> None:
        # Connect to broker
        broker = self.broker

        # Cancel all open orders
        broker.cancel_orders()
