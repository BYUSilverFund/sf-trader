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
    print("[1] Getting universe...")
    universe = utils.data.get_universe()
    print(f"[1] Done — {len(universe)} tickers")

    # Get account value
    print("[2] Getting account value...")
    account_value = broker.get_account_value()
    print(f"[2] Done — ${account_value:,.2f}")

    # Get prices
    print("[3] Getting prices...")
    prices = utils.data.get_prices(tickers=universe)
    print(f"[3] Done — {prices.shape[0]} rows")

    # Get tradable universe
    tradable_universe = utils.functions.get_tradable_universe(prices=prices)
    print(f"[4] Tradable universe — {len(tradable_universe)} tickers")

    # Get asset data
    print("[5] Getting asset data...")
    assets = utils.data.get_assets(tickers=tradable_universe)
    print(f"[5] Done — {assets.shape[0]} rows")

    # Get alphas
    print("[6] Getting alphas...")
    alphas = utils.functions.get_alphas(assets=assets)
    print(f"[6] Done")

    # Get betas
    print("[7] Getting betas...")
    betas = utils.data.get_betas(tickers=tradable_universe)
    print(f"[7] Done")

    # Get covariance matrix
    print("[8] Getting covariance matrix...")
    covariance_matrix = utils.data.get_covariance_matrix(
        tickers=tradable_universe
    )
    print(f"[8] Done")

    # Get optimal weights
    print(f"[9] Running optimizer on {len(tradable_universe)} tickers...")
    optimal_weights = utils.functions.get_optimal_weights(
        tickers=tradable_universe,
        alphas=alphas,
        betas=betas,
        covariance_matrix=covariance_matrix,
    )
    print(f"[9] Done — {len(optimal_weights)} positions")

    # Get optimal shares
    print("[10] Getting optimal shares...")
    optimal_shares = utils.functions.get_optimal_shares(
        weights=optimal_weights, prices=prices, account_value=account_value
    )
    print(f"[10] Done")

    # Disconnect broker
    del broker
    del config.broker

    return Shares.validate(optimal_shares)
