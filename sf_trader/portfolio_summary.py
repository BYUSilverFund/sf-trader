import dataframely as dy
from sf_trader.components.models import Shares
from sf_trader.config import Config
import sf_trader.utils.data
from rich.console import Console
import sf_trader.ui.tables
import sf_trader.utils.functions

def get_portfolio_summary(shares: dy.DataFrame[Shares], config: Config) -> None:
    # Connect to broker
    broker = config.broker

    # Configure modules
    sf_trader.utils.data.set_config(config=config)

    # Get account value
    account_value = broker.get_account_value()

    # Get tickers
    tickers = shares["ticker"].to_list()

    # Get prices
    prices = sf_trader.utils.data.get_prices(tickers=tickers)

    # Get dollars
    dollars = sf_trader.utils.functions.get_dollars(shares=shares, prices=prices)
    dollars_allocated = dollars['dollars'].sum()

    # Calculate portfolio weights from dollars
    weights = sf_trader.utils.functions.get_weights_from_dollars(dollars=dollars, account_value=account_value)

    # Get benchmark weights
    benchmark = sf_trader.utils.data.get_benchmark_weights()

    # Get universe
    universe = benchmark["ticker"].sort().to_list()

    # Get covariance matrix
    covariance_matrix = sf_trader.utils.data.get_covariance_matrix(tickers=universe)

    # Decompose weights
    total_weights, active_weights = sf_trader.utils.functions.decompose_weights(
        benchmark=benchmark, weights=weights
    )

    # Generate portfolio metrics table
    portfolio_metrics = sf_trader.utils.functions.get_portfolio_metrics(
        total_weights=total_weights,
        active_weights=active_weights,
        covariance_matrix=covariance_matrix,
        account_value=account_value,
        dollars_allocated=dollars_allocated
    )
    portfolio_metrics_table = sf_trader.ui.tables.generate_portfolio_metrics_table(portfolio_metrics)

    # Generate top long positiosn table
    top_long_positions = sf_trader.utils.functions.get_top_long_positions(
        shares=shares,
        prices=prices,
        dollars=dollars,
        weights=weights,
        benchmark=benchmark,
        account_value=account_value,
    )
    top_long_positions_table = sf_trader.ui.tables.generate_positions_table(positions=top_long_positions, title='Top 10 Long Positions')

    # Render UI
    console = Console()
    console.print()
    console.print(portfolio_metrics_table)
    console.print()
    console.print(top_long_positions_table)

    del broker
    del config.broker