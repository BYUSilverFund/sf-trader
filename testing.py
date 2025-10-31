from sf_trader.components.models import Shares
import dataframely as dy
from sf_trader.config import Config
import sf_trader.utils.data
import sf_trader.utils.functions
from sf_trader.components.models import Assets, Alphas
import datetime as dt
import sf_quant.data as sfd
import polars as pl
import sf_trader.utils.data
import numpy as np

_config = Config("config.yml")
config = Config("config.yml")


def get_assets(tickers: list[str]) -> dy.DataFrame[Assets]:
    lookback_days = max([signal.lookback_days for signal in _config.signals])
    start_date = _config.data_date - dt.timedelta(days=lookback_days)
    end_date = _config.data_date

    columns = [
        "date",
        "barrid",
        "ticker",
        "return",
        "predicted_beta",
        "specific_risk",
    ]

    asset_data = (
        sfd.load_assets(
            start=start_date, end=end_date, columns=columns, in_universe=True
        )
        .with_columns(pl.col("return").truediv(100))
        .filter(pl.col("ticker").is_in(tickers))
        .sort("barrid", "date")
    )

    return asset_data #Assets.validate(asset_data)

def get_alphas(assets: dy.DataFrame[Assets]) -> dy.DataFrame[Alphas]:
    signals = _config.signals
    signal_combinator = _config.signal_combinator
    ic = _config.ic
    data_date = _config.data_date

    alphas = (
        assets.sort("barrid", "date")
        # Compute signals
        .with_columns([signal.expr for signal in signals])
        # Compute scores
        .with_columns(
            [
                pl.col(signal.name)
                .sub(pl.col(signal.name).mean())
                .truediv(pl.col(signal.name).std())
                for signal in signals
            ]
        )
        # Compute alphas
        .with_columns(
            [
                pl.col(signal.name).mul(pl.lit(ic)).mul(pl.col("specific_risk"))
                for signal in signals
            ]
        )
        # Fill null alphas with 0
        .with_columns(pl.col(signal.name).fill_null(0) for signal in signals)
        # Combine alphas
        .with_columns(signal_combinator.combine_fn([signal.name for signal in signals]))
        # Get trade date
        .filter(pl.col("date").eq(data_date))
        .select("ticker", "alpha")
        .sort("ticker")
    )

    return Alphas.validate(alphas)

def get_covariance_matrix(tickers: list[str]) -> np.ndarray:
    ids = (
        sf_trader.utils.data.get_ticker_barrid_mapping()
        .join(pl.DataFrame({"ticker": tickers}), on="ticker", how="inner")
        .sort("ticker")
    )
    tickers_ = ids["ticker"].to_list()
    barrids = sorted(ids["barrid"].to_list())

    mapping = {barrid: ticker for barrid, ticker in zip(barrids, tickers_)}

    covariance_matrix = (
        sfd.construct_covariance_matrix(date_=_config.data_date, barrids=barrids)
        .with_columns(pl.col("barrid").replace(mapping))
        .rename(mapping | {"barrid": "ticker"})
        .sort("ticker")
    )
    print(covariance_matrix)
    return covariance_matrix.drop("ticker").to_numpy()


# Connect to broker
broker = config.broker

# Config data loader
sf_trader.utils.data.set_config(config=config)
sf_trader.utils.functions.set_config(config=config)

# Get universe
universe = sf_trader.utils.data.get_universe()

# Get account value
account_value = broker.get_account_value()

# Get prices
prices = sf_trader.utils.data.get_prices(tickers=universe)

# Get tradable universe
tradable_universe = sf_trader.utils.functions.get_tradable_universe(prices=prices)

# Get asset data
assets = get_assets(tickers=tradable_universe)

# Get alphas
alphas = get_alphas(assets=assets)
print(alphas.sort('alpha'))

# Get betas
betas = sf_trader.utils.data.get_betas(tickers=tradable_universe)

# Get covariance matrix
covariance_matrix = get_covariance_matrix(
    tickers=tradable_universe
)

# # Get optimal weights
# optimal_weights = sf_trader.utils.functions.get_optimal_weights(
#     tickers=tradable_universe,
#     alphas=alphas,
#     betas=betas,
#     covariance_matrix=covariance_matrix,
# )

# # Get optimal shares
# optimal_shares = sf_trader.utils.functions.get_optimal_shares(
#     weights=optimal_weights, prices=prices, account_value=account_value
# )

# Disconnect broker
del broker
del config.broker

# print(optimal_weights.sort('weight'))
