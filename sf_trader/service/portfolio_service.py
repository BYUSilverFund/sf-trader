from sf_trader.config import Config
import sf_trader.utils.computations as computations

from sf_trader.dal.dao.portfolio_dao import PortfolioDAO
from sf_trader.dal.dao.surface_dao import SurfaceDAO
from sf_trader.dal.models.schema_models import OrdersDF, OrdersSchema


class PortfolioService:
    def __init__(
        self, 
        config: Config, 
        portfolio_dao: PortfolioDAO | None = None, 
        surface_dao: SurfaceDAO | None = None
    ):
        self.portfolio_dao = portfolio_dao or PortfolioDAO()
        self.surface_dao = surface_dao or SurfaceDAO(config)
        self.config = config
        self.broker = config.broker


    def get_portfolio(self, config: Config) -> SharesDF:

        sf_trader.domain.computations.set_config(config=self.config)

        # Get universe
        universe = self.portfolio_dao.get_universe_by_date(date=config.data_date)

        # Get account value
        account_value = self.broker.get_account_value()

        # Get prices
        prices = self.portfolio_dao.get_prices_by_date(date=config.data_date, tickers=universe)

        # Get optimal weights
        optimal_weights = port_dao.get_optimal_weights_by_date(date=config.data_date)

        # Get optimal shares
        optimal_shares = sf_trader.domain.computations.get_optimal_shares(
            weights=optimal_weights, prices=prices, account_value=account_value
        )

        # Disconnect broker
        del broker
        del config.broker
        del port_dao

        return SharesSchema.validate(optimal_shares)