from sf_trader.components.models import Shares
import dataframely as dy
import polars as pl
from sf_trader.config import Config
from sf_trader.broker.test import TestClient
import sf_trader.data
import sf_trader.functions


def get_portfolio(config: Config) -> dy.DataFrame[Shares]:
    # Connect to broker
    broker = TestClient(config=config)

    # Config data loader
    sf_trader.data.set_config(config=config)
    sf_trader.functions.set_config(config=config)

    # Get tradable universe
    universe = sf_trader.data.get_universe()

    # Get account value
    account_value = broker.get_account_value()

    # Get prices
    prices = broker.get_prices(tickers=universe)

    # Get tradable universe
    tradable_universe = sf_trader.functions.get_tradable_universe(prices=prices)

    # Get asset data
    assets = sf_trader.data.get_assets(tickers=tradable_universe)

    # Get alphas
    alphas = sf_trader.functions.get_alphas(assets=assets)

    # Get betas
    betas = sf_trader.data.get_betas(tickers=tradable_universe)

    # Get optimal weights
    optimal_weights = sf_trader.functions.get_optimal_weights(
        tickers=tradable_universe,
        alphas=alphas,
        betas=betas,
    )

    # Get optimal shares
    optimal_shares = sf_trader.functions.get_optimal_shares(
        weights=optimal_weights, prices=prices, account_value=account_value
    )

    return Shares.validate(optimal_shares)
