from sf_trader.config import Config
from rich.console import Console
import sf_trader.domain.tables_ui
import sf_trader.domain.computations

from sf_trader.dal.dao.portfolio_dao import PortfolioDAO
from sf_trader.dal.models.schema_models import SharesDF


def get_portfolio_summary(shares: SharesDF, config: Config) -> None:
    # Connect to broker
    broker = config.broker

    # Connect to database
    port_dao = PortfolioDAO()

    # Configure modules
    sf_trader.utils.data.set_config(config=config)

    # Get account value
    account_value = broker.get_account_value()

    # Get tickers
    tickers = shares["ticker"].to_list()

    # Get prices
    prices = port_dao.get_prices_by_date(date=config.data_date, tickers=tickers)

    # Get dollars
    dollars = sf_trader.domain.computations.get_dollars(shares=shares, prices=prices)
    dollars_allocated = dollars["dollars"].sum()

    # Calculate portfolio weights from dollars
    weights = sf_trader.domain.computations.get_weights_from_dollars(
        dollars=dollars, account_value=account_value
    )

    # Get benchmark weights
    benchmark = port_dao.get_benchmark_weights_by_date(date=config.data_date)

    # Get universe
    universe = benchmark["ticker"].sort().to_list()

    # Get covariance matrix
    covariance_matrix = sf_trader.utils.data.get_covariance_matrix(tickers=universe)

    # Decompose weights
    total_weights, active_weights = sf_trader.domain.computations.decompose_weights(
        benchmark=benchmark, weights=weights
    )

    # Generate portfolio metrics table
    portfolio_metrics = sf_trader.domain.computations.get_portfolio_metrics(
        total_weights=total_weights,
        active_weights=active_weights,
        covariance_matrix=covariance_matrix,
        account_value=account_value,
        dollars_allocated=dollars_allocated,
    )
    portfolio_metrics_table = sf_trader.domain.tables_ui.generate_portfolio_metrics_table(
        portfolio_metrics
    )

    # Generate top long positiosn table
    top_long_positions = sf_trader.domain.computations.get_top_long_positions(
        shares=shares,
        prices=prices,
        dollars=dollars,
        weights=weights,
        benchmark=benchmark,
        account_value=account_value,
    )
    top_long_positions_table = sf_trader.domain.tables_ui.generate_positions_table(
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
