import dataframely as dy
from components.models import Shares
from config import Config
import utils.data
from rich.console import Console
import ui.tables
import utils.functions


def get_portfolio_summary(shares: dy.DataFrame[Shares], config: Config) -> None:
    # Connect to broker
    broker = config.broker

    # Configure modules
    utils.data.set_config(config=config)

    # Get account value
    account_value = broker.get_account_value()

    # Get tickers
    tickers = shares["ticker"].to_list()

    # Get prices
    prices = utils.data.get_prices(tickers=tickers)

    # Get dollars
    dollars = utils.functions.get_dollars(shares=shares, prices=prices)
    dollars_allocated = dollars["dollars"].sum()

    # Calculate portfolio weights from dollars
    weights = utils.functions.get_weights_from_dollars(
        dollars=dollars, account_value=account_value
    )

    # Get benchmark weights
    benchmark = utils.data.get_benchmark_weights()

    # Get universe
    universe = benchmark["ticker"].sort().to_list()

    # Get covariance matrix
    covariance_matrix = utils.data.get_covariance_matrix(tickers=universe)

    # Decompose weights
    total_weights, active_weights = utils.functions.decompose_weights(
        benchmark=benchmark, weights=weights
    )

    # Generate portfolio metrics table
    portfolio_metrics = utils.functions.get_portfolio_metrics(
        total_weights=total_weights,
        active_weights=active_weights,
        covariance_matrix=covariance_matrix,
        account_value=account_value,
        dollars_allocated=dollars_allocated,
    )
    portfolio_metrics_table = ui.tables.generate_portfolio_metrics_table(
        portfolio_metrics
    )

    # Generate top long positiosn table
    top_long_positions = utils.functions.get_top_long_positions(
        shares=shares,
        prices=prices,
        dollars=dollars,
        weights=weights,
        benchmark=benchmark,
        account_value=account_value,
    )
    top_long_positions_table = ui.tables.generate_positions_table(
        positions=top_long_positions, title="Top 10 Long Positions"
    )

    # Render UI
    console = Console()
    console.print()
    console.print(portfolio_metrics_table)
    console.print()
    console.print(top_long_positions_table)

    del broker
    del config.broker
