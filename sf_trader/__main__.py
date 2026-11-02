import datetime as dt
from pathlib import Path

import click
import polars as pl

from sf_trader import charts, data, metrics
from sf_trader.combine import combine_alphas
from sf_trader.config import Config, ConfigError, load_config
from sf_trader.ibkr import IBKRClient
from sf_trader.optimize import OptimizationError, optimize_portfolio
from sf_trader.orders import compute_orders, write_basket
from sf_trader.universe import apply_do_not_trade, apply_universe_filter

PORTFOLIO_PATH = Path("portfolio.csv")
ORDERS_PATH = Path("orders.csv")
PORTFOLIO_CHART_PATH = Path("portfolio_metrics.png")
ORDER_CHART_PATH = Path("order_metrics.png")


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
        portfolio.write_csv(PORTFOLIO_PATH)

        tickers = sorted(set(portfolio["ticker"]) | set(positions["ticker"]) - set(config.do_not_trade))
        print(f"Fetching quotes for {len(tickers)} tickers...")
        quotes = client.get_quotes(tickers)

    orders, skipped_no_quote, skipped_small = compute_orders(
        portfolio,
        positions,
        quotes,
        config.orders,
        capital,
        list(config.do_not_trade),
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
def cli(config_path: Path, trade_date: dt.datetime | None) -> None:
    """Build the portfolio and write a BasketTrader orders file. Never places orders."""
    try:
        config = load_config(config_path, trade_date.date() if trade_date else None)
        run(config)
    except (ConfigError, data.DataError, OptimizationError) as e:
        raise click.ClickException(str(e)) from e


if __name__ == "__main__":
    cli()
