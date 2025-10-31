from sf_trader.components.models import Shares
import dataframely as dy
from sf_trader.config import Config
import sf_trader.utils.data
import sf_trader.utils.functions


def get_portfolio(config: Config) -> dy.DataFrame[Shares]:
    # Connect to broker
    broker = config.broker

    # Config data loader
    sf_trader.utils.data.set_config(config=config)
    sf_trader.utils.functions.set_config(config=config)

    # Get tradable universe
    universe = sf_trader.utils.data.get_universe()

    # Get account value
    account_value = broker.get_account_value()

    # Get prices
    prices = sf_trader.utils.data.get_prices(tickers=universe)

    # Get tradable universe
    tradable_universe = sf_trader.utils.functions.get_tradable_universe(prices=prices)

    # Get asset data
    assets = sf_trader.utils.data.get_assets(tickers=tradable_universe)

    # Get alphas
    alphas = sf_trader.utils.functions.get_alphas(assets=assets)

    # Get betas
    betas = sf_trader.utils.data.get_betas(tickers=tradable_universe)

    # Get covariance matrix
    covariance_matrix = sf_trader.utils.data.get_covariance_matrix(
        tickers=tradable_universe
    )

    # Get optimal weights
    optimal_weights = sf_trader.utils.functions.get_optimal_weights(
        tickers=tradable_universe,
        alphas=alphas,
        betas=betas,
        covariance_matrix=covariance_matrix,
    )

    # Get optimal shares
    optimal_shares = sf_trader.utils.functions.get_optimal_shares(
        weights=optimal_weights, prices=prices, account_value=account_value
    )

    # Disconnect broker
    del broker
    del config.broker

    return Shares.validate(optimal_shares)
