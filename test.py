import sf_quant.data as sfd
import datetime as dt
import polars as pl


from sf_trader.components.combinators import get_combinator
from sf_trader.components.signals import get_signal


columns = [
        "date",
        "barrid",
        "ticker",
        "price",
        "return",
        "predicted_beta",
        "specific_risk",
        "specific_return",
        "daily_volume"
    ]

data_date = dt.date(2025,12,31)


def get_universe() -> list[str]:
    return (
        sfd.load_assets_by_date(
            date_=data_date, columns=["ticker"], in_universe=True
        )["ticker"]
        .unique()
        .sort()
        .to_list()
    )

def get_assets(tickers: list[str]) -> pl.DataFrame:
    # signal_names = ["momentum", "reversal", "beta"]
    signal_names = ["vol_scaled_barra_momentum", "barra_reversal_volume_clipped", "ivol"]
    signals = [get_signal(name) for name in signal_names]
    lookback_days = max([signal.lookback_days for signal in signals])
    start_date = data_date - dt.timedelta(days=lookback_days)
    end_date = data_date

    columns = [
        "date",
        "barrid",
        "ticker",
        "price",
        "return",
        "predicted_beta",
        "specific_risk",
        "specific_return",
        "daily_volume"
    ]

    asset_data = (
        sfd.load_assets(
            start=start_date, end=end_date, columns=columns, in_universe=True
        )
        .with_columns(
            pl.col("return").truediv(100),
            pl.col("specific_return").truediv(100),
            pl.col("specific_risk").truediv(100),
        )
        .filter(pl.col("ticker").is_in(tickers))
        .sort("ticker", "date")
    )

    return asset_data

# assets = sfd.load_assets_by_date(date_, in_universe=True, columns=columns)
universe = get_universe()
assets = get_assets(universe)

def clip_scores_winsorize(signal_name: str) -> pl.Expr:
    if signal_name == "barra_reversal_volume_clipped":
        return pl.col(signal_name).clip(-2.0, 2.0)
    return pl.col(signal_name)


def barra_reversal_volume_clipped_alpha(signal_name: str, ic: float) -> pl.Expr:
    score = pl.col(signal_name).clip(lower_bound=-2.0, upper_bound=2.0)

    dv = (pl.col("daily_volume") * pl.col("price")).log1p()

    dv_mean = dv.rolling_mean(window_size=252, min_samples=1).over("barrid")
    dv_std = dv.rolling_std(window_size=252, min_samples=2).over("barrid")

    volume_score = (
        (dv - dv_mean)
        / dv_std.fill_null(1.0).clip(lower_bound=0.0001)
    ).fill_null(0.0)

    gk_alpha = score * pl.lit(ic) * pl.col("specific_risk")

    alpha = (
        pl.when((score >= 2.0) & (volume_score >= 2.0))
        .then(0.0)
        .otherwise(gk_alpha)
    )

    return alpha.alias(signal_name)


def get_alphas(assets) -> pl.DataFrame:
    # signal_names = ["momentum", "reversal", "beta"]
    signal_names = ["vol_scaled_barra_momentum", "barra_reversal_volume_clipped", "ivol"]
    signals = [get_signal(name) for name in signal_names]
    signal_combinator = get_combinator("mean")
    ic = 0.5
    data_date = dt.date.fromisoformat("2025-12-31")

    alphas = (
        assets.sort("ticker", "date")
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
        # # Windsorize barra_reversal_volume_clipped
        # .with_columns(
        #     [clip_scores_winsorize(signal.name) for signal in signals]
        # )
        # Compute alphas
        .with_columns(
            [
                barra_reversal_volume_clipped_alpha(signal.name, ic)
                if signal.name == "barra_reversal_volume_clipped"
                else pl.col(signal.name).mul(pl.lit(ic)).mul(pl.col("specific_risk"))
                for signal in signals
            ]
        )
        # Fill null alphas with 0
        .with_columns(pl.col(signal.name).fill_null(0) for signal in signals)
        # Combine alphas
        .with_columns(signal_combinator.combine_fn([signal.name for signal in signals]))
        # Get trade date
        .filter(pl.col("date").eq(data_date))
        # .select("ticker", "alpha")
        .sort("ticker")
    )

    # print(alphas)
    return alphas

if __name__ == "__main__":
    alphas = get_alphas(assets)
    with pl.Config(tbl_cols=-1):
        print(alphas)