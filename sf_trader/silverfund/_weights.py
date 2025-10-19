import polars as pl
import sf_quant.optimizer as sfo
import datetime as dt
import sf_quant.data as sfd

def compute_optimal_weights(data: pl.DataFrame, gamma: float, trade_date: dt.date, decimal_places: int):
    data = data.sort('barrid')
    
    barrids = data['barrid'].to_list()
    alphas = data['alpha'].to_list()
    betas = data['predicted_beta'].to_list()

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
            other=data.select('barrid', 'ticker'),
            on='barrid',
            how='left'
        )
        .sort('barrid', 'weight')
    )