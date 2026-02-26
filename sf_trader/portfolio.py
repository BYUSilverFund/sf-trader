from sf_trader.config import Config
import sf_trader.domain.computations

from sf_trader.dal.dao.portfolio_dao import PortfolioDAO
from sf_trader.dal.models.schema_models import SharesDF, SharesSchema


def get_portfolio(config: Config) -> SharesDF:
    # Connect to broker
    broker = config.broker

    # Connect to database
    port_dao = PortfolioDAO()

    # Config data loader
    sf_trader.utils.data.set_config(config=config)
    sf_trader.domain.computations.set_config(config=config)

    # Get universe
    universe = port_dao.get_universe_by_date(date=config.data_date)

    # Get account value
    account_value = broker.get_account_value()

    # Get prices
    prices = port_dao.get_prices_by_date(date=config.data_date, tickers=universe)

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
