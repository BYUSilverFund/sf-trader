import polars as pl
from sf_trader.models import Config
import sf_quant.data as sfd
import sf_quant.optimizer as sfo
import datetime as dt

def get_tradable_tickers(df: pl.DataFrame) -> list[str]:
    return (
        df
        .filter(
            pl.col('price').ge(5)
        )
        ['ticker']
        .unique()
        .sort()
        .to_list()
    )

def get_trades(weights: pl.DataFrame, prices: pl.DataFrame, config: Config, available_funds: float) -> list[dict]:
    return (
        weights
        .join(
            prices,
            on='ticker',
            how='left'
        )
        .with_columns(
            pl.lit(available_funds).mul(pl.col('weight')).alias('dollars')
        )
        .with_columns(
            pl.col('dollars').truediv(pl.col('price')).floor().alias('shares')
        )
        .select(
            'ticker',
            'price',
            'shares'
        )
        .to_dicts()
    )
def get_alphas(df: pl.DataFrame, config: Config) -> pl.DataFrame:
    signals = config.signals
    signal_combinator = config.signal_combinator
    ic = config.ic

    return (
        df
        .sort('barrid', 'date')
        # Compute signals
        .with_columns([signal.expr for signal in signals])
        # Compute scores
        .with_columns([
            pl.col(signal.name).sub(pl.col(signal.name).mean()).truediv(pl.col(signal.name).std())
            for signal in signals
        ])
        # Compute alphas
        .with_columns([
            pl.col(signal.name).mul(pl.lit(ic)).mul(pl.col('specific_risk'))
            for signal in signals
        ])
        # Fill null alphas with 0
        .with_columns(
            pl.col(signal.name).fill_null(0)
            for signal in signals    
        )
        # Combine alphas
        .with_columns(
            signal_combinator.combine_fn([signal.name for signal in signals])
        )
        .select(
            'date',
            'barrid',
            'ticker',
            'predicted_beta',
            'alpha'
        )
        .sort('barrid', 'date')
    )

def get_portfolio_weights(df: pl.DataFrame, config: Config, trade_date: dt.date) -> pl.DataFrame:
    df = df.sort('barrid')

    gamma = config.gamma
    decimal_places = config.decimal_places

    barrids = df['barrid'].to_list()
    alphas = df['alpha'].to_list()
    betas = df['predicted_beta'].to_list()

    covariance_matrix = (
        sfd.construct_covariance_matrix(
            date_=trade_date,
            barrids=barrids
        )
        .drop('barrid')
        .to_numpy()
    )

    constraints = [
        sfo.FullInvestment(),
        sfo.NoBuyingOnMargin(),
        sfo.LongOnly(),
        sfo.UnitBeta()
    ]

    weights = sfo.mve_optimizer(
        ids=barrids,
        alphas=alphas,
        betas=betas,
        covariance_matrix=covariance_matrix,
        gamma=gamma,
        constraints=constraints
    )

    return (
        weights
        .with_columns(pl.col("weight").round(4))
        .filter(pl.col("weight").ge(1 * 10**-decimal_places))
        .join(
            other=df.select('barrid', 'ticker'),
            on='barrid',
            how='left'
        )
        .sort('barrid', 'weight')
    )