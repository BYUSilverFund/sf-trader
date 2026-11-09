import datetime as dt
from dataclasses import replace
from pathlib import Path

import click
import polars as pl

from sf_trader import charts, data, metrics
from sf_trader.combine import combine_alphas
from sf_trader.config import Config, ConfigError, load_config
from sf_trader.ibkr import IBKRClient
from sf_trader.models import Portfolio
from sf_trader.optimize import OptimizationError, optimize_portfolio
from sf_trader.orders import compute_orders, compute_targets, write_basket
from sf_trader.universe import apply_do_not_trade, apply_universe_filter

PORTFOLIO_PATH = Path("portfolio.csv")
ORDERS_PATH = Path("orders.csv")
REFRESH_PATH = Path("orders_refresh.csv")
PORTFOLIO_CHART_PATH = Path("portfolio_metrics.png")
ORDER_CHART_PATH = Path("order_metrics.png")


class RefreshError(Exception):
    """Raised when orders cannot be safely generated or refreshed"""


def require_no_working_orders(client: IBKRClient) -> None:
    """New orders on top of working ones would trade the same shares twice."""
    if tickers := client.get_working_order_tickers():
        raise RefreshError(
            f"{len(tickers)} working orders in TWS: {tickers}. "
            "Cancel them in TWS and run again."
        )


def get_capital(config: Config, client: IBKRClient, frozen_value: float) -> float:
    capital = config.orders.capital
    if capital == "net_liq":
        return client.get_net_liquidation() - frozen_value
    return capital


def get_frozen_value(config: Config, client: IBKRClient, positions: pl.DataFrame) -> float:
    """Market value of do-not-trade holdings, which the optimizer cannot reallocate."""
    frozen = positions.filter(pl.col("ticker").is_in(config.do_not_trade))
    if frozen.is_empty():
        return 0.0

    quotes = client.get_quotes(frozen["ticker"].to_list())
    valued = frozen.join(quotes, on="ticker", how="left").with_columns(
        pl.coalesce("last", "close", "bid").alias("price")
    )
    if unpriced := valued.filter(pl.col("price").is_null())["ticker"].to_list():
        print(f"WARNING: valuing do-not-trade holdings at 0, no price: {unpriced}")
    return float((valued["shares"] * valued["price"]).sum())


def run(config: Config) -> None:
    signals = list(config.signals)

    alpha_date = data.resolve_alpha_date(config.trade_date)
    print(f"Trade date {config.trade_date}, alphas from {alpha_date}")

    alphas = data.load_alphas(alpha_date, signals)
    assets = data.load_assets(alpha_date)
    exposure_barrids = data.load_exposure_barrids(alpha_date)

    filtered, dropped = apply_universe_filter(
        alphas, assets, config.min_price, exposure_barrids
    )
    if any(dropped.values()):
        print(f"WARNING: universe filter dropped {sum(dropped.values())} names: {dropped}")
    print(f"Universe: {filtered.height} of {alphas.height} names")

    for signal in signals:
        coverage = filtered[signal].is_not_null().mean()
        if coverage < 0.9:
            print(f"WARNING: {signal} covers only {coverage:.1%} of the universe")

    combined = combine_alphas(filtered, signals, config.combination_strategy)
    tradeable, excluded = apply_do_not_trade(combined, list(config.do_not_trade))
    print(f"Do-not-trade: excluded {excluded} names from the optimizer")

    # Connect before the slow solve so a TWS problem fails fast. Read-only.
    with IBKRClient(config.ibkr) as client:
        require_no_working_orders(client)
        positions = client.get_positions()
        capital = get_capital(config, client, get_frozen_value(config, client, positions))
        print(f"Capital ${capital:,.2f}, {positions.height} current positions")

        barrids = tradeable["barrid"].to_list()
        print(f"Optimizing {len(barrids)} names...")
        portfolio = optimize_portfolio(
            tradeable,
            data.load_covariance_matrix(alpha_date, barrids),
            config.constraints,
            capital,
        )

        tickers = sorted(set(portfolio["ticker"]) | set(positions["ticker"]) - set(config.do_not_trade))
        print(f"Fetching quotes for {len(tickers)} tickers...")
        quotes = client.get_quotes(tickers)

    portfolio = compute_targets(portfolio, quotes, config.orders, capital)
    portfolio.write_csv(PORTFOLIO_PATH)

    orders, skipped_no_quote, skipped_small = compute_orders(
        portfolio, positions, quotes, config.orders, list(config.do_not_trade)
    )
    write_basket(orders, ORDERS_PATH, f"SF_{config.trade_date:%Y%m%d}")

    if skipped_no_quote:
        print(f"WARNING: skipped {len(skipped_no_quote)} tickers with no usable quote: {skipped_no_quote}")
    no_tick = quotes.filter(pl.col("min_tick").is_null()).height
    if no_tick:
        print(f"WARNING: no tick size for {no_tick} tickers, using the default")

    benchmark_barrids = (
        assets.filter(pl.col("in_universe").fill_null(False))
        .filter(
            pl.col("ticker").is_not_null(),
            pl.col("market_cap").is_not_null(),
            pl.col("specific_risk").is_not_null(),
            pl.col("barrid").is_in(exposure_barrids),
        )["barrid"]
        .to_list()
    )
    benchmark_frame = metrics.benchmark_weights(assets, benchmark_barrids)
    benchmark = metrics.Benchmark(
        tickers=benchmark_frame["ticker"].to_list(),
        weights=benchmark_frame["weight"].to_numpy(),
        covariance_matrix=data.load_covariance_matrix(alpha_date, benchmark_barrids),
    )

    portfolio_metrics = metrics.get_portfolio_metrics(portfolio, benchmark, capital)
    order_metrics = metrics.get_order_metrics(
        portfolio,
        positions,
        orders,
        assets,
        benchmark,
        capital,
        len(skipped_no_quote),
        skipped_small,
    )

    title = f"Trade date {config.trade_date}, alphas from {alpha_date}"
    charts.save_portfolio_chart(portfolio, portfolio_metrics, PORTFOLIO_CHART_PATH, title)
    charts.save_order_chart(orders, order_metrics, ORDER_CHART_PATH, title)

    print(
        f"Portfolio: {portfolio.height} positions, active risk "
        f"{portfolio_metrics.active_risk:.2%}, total risk {portfolio_metrics.total_risk:.2%}"
    )
    print(
        f"Orders: {order_metrics.num_buys} buys ${order_metrics.buy_value:,.0f}, "
        f"{order_metrics.num_sells} sells ${order_metrics.sell_value:,.0f}, "
        f"TC after orders {order_metrics.tc_after_orders:.3f}"
    )
    print(f"Wrote {PORTFOLIO_PATH}, {ORDERS_PATH}, {PORTFOLIO_CHART_PATH}, {ORDER_CHART_PATH}")


def load_saved_portfolio(config: Config) -> pl.DataFrame:
    """Targets from the original run, so a refresh trades the same portfolio."""
    if not PORTFOLIO_PATH.exists() or not ORDERS_PATH.exists():
        raise RefreshError(f"Refresh needs {PORTFOLIO_PATH} and {ORDERS_PATH} from a full run")

    tags = pl.read_csv(ORDERS_PATH, columns=["BasketTag"])["BasketTag"].unique().to_list()
    expected = f"SF_{config.trade_date:%Y%m%d}"
    if tags != [expected]:
        raise RefreshError(
            f"{ORDERS_PATH} is tagged {tags}, not {expected}. Do a full run for "
            f"{config.trade_date}, or pass the --trade-date it was built for."
        )

    portfolio = pl.read_csv(PORTFOLIO_PATH)
    if "target_shares" not in portfolio.columns:
        raise RefreshError(
            f"{PORTFOLIO_PATH} has no target_shares column, so it predates refresh. "
            "Its shares are estimates and would create false trades. Do a full run."
        )
    return Portfolio.validate(portfolio, cast=True)


def refresh(config: Config, aggressiveness: float | None) -> None:
    """Reprice what is still unfilled against the original targets."""
    portfolio = load_saved_portfolio(config)
    settings = replace(
        config.orders,
        limit_aggressiveness=(
            config.orders.refresh_aggressiveness
            if aggressiveness is None
            else aggressiveness
        ),
    )

    with IBKRClient(config.ibkr) as client:
        require_no_working_orders(client)
        positions = client.get_positions()
        tickers = sorted(set(portfolio["ticker"]) | set(positions["ticker"]) - set(config.do_not_trade))
        print(f"Fetching quotes for {len(tickers)} tickers...")
        quotes = client.get_quotes(tickers)

    orders, skipped_no_quote, skipped_small = compute_orders(
        portfolio,
        positions,
        quotes,
        settings,
        list(config.do_not_trade),
        toward_market=True,
    )
    write_basket(
        orders,
        REFRESH_PATH,
        f"SF_{config.trade_date:%Y%m%d}_R{dt.datetime.now():%H%M}",
    )

    if skipped_no_quote:
        print(f"WARNING: skipped {len(skipped_no_quote)} tickers with no usable quote: {skipped_no_quote}")
    if skipped_small:
        print(f"Left {skipped_small} trades below min_trade_value")

    buys = orders.filter(pl.col("action").eq("BUY"))
    sells = orders.filter(pl.col("action").eq("SELL"))
    print(
        f"Remaining at aggressiveness {settings.limit_aggressiveness}: "
        f"{buys.height} buys ${buys['value'].sum():,.0f}, "
        f"{sells.height} sells ${sells['value'].sum():,.0f}"
    )
    print(f"Wrote {REFRESH_PATH}")


@click.command()
@click.option(
    "--config",
    "config_path",
    required=True,
    type=click.Path(exists=True, path_type=Path),
    help="Path to the YAML configuration file",
)
@click.option(
    "--trade-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="Override trade_date from the config",
)
@click.option(
    "--refresh",
    "refresh_orders",
    is_flag=True,
    help="Reprice unfilled orders against the saved portfolio into orders_refresh.csv",
)
@click.option(
    "--aggressiveness",
    type=click.FloatRange(0, 1),
    default=None,
    help="With --refresh, override orders.refresh_aggressiveness",
)
def cli(
    config_path: Path,
    trade_date: dt.datetime | None,
    refresh_orders: bool,
    aggressiveness: float | None,
) -> None:
    """Build the portfolio and write a BasketTrader orders file. Never places orders."""
    if aggressiveness is not None and not refresh_orders:
        raise click.UsageError("--aggressiveness only applies with --refresh")

    try:
        config = load_config(config_path, trade_date.date() if trade_date else None)
        if refresh_orders:
            refresh(config, aggressiveness)
        else:
            run(config)
    except (ConfigError, data.DataError, OptimizationError, RefreshError) as e:
        raise click.ClickException(str(e)) from e


if __name__ == "__main__":
    cli()
