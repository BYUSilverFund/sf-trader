from components.models import Shares
import dataframely as dy
from config import Config
import utils.data
import utils.functions


def get_portfolio(config: Config) -> dy.DataFrame[Shares]:
    # Connect to broker
    broker = config.broker

    # Config data loader
    utils.data.set_config(config=config)
    utils.functions.set_config(config=config)

    # Get universe
    universe = utils.data.get_universe()

    # Get account value
    account_value = broker.get_account_value()

    # Get prices
    prices = utils.data.get_prices(tickers=universe)

    # Get tradable universe
    tradable_universe = utils.functions.get_tradable_universe(prices=prices)

    # Get asset data
    assets = utils.data.get_assets(tickers=tradable_universe)

    # Get alphas
    alphas = utils.functions.get_alphas(assets=assets)

    # Get betas
    betas = utils.data.get_betas(tickers=tradable_universe)

    # Get covariance matrix
    covariance_matrix = utils.data.get_covariance_matrix(
        tickers=tradable_universe
    )

    # Get optimal weights
    optimal_weights = utils.functions.get_optimal_weights(
        tickers=tradable_universe,
        alphas=alphas,
        betas=betas,
        covariance_matrix=covariance_matrix,
    )

    # Get optimal shares
    optimal_shares = utils.functions.get_optimal_shares(
        weights=optimal_weights, prices=prices, account_value=account_value
    )

    # Disconnect broker
    del broker
    del config.broker

    return Shares.validate(optimal_shares)
