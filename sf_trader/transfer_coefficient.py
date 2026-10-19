import dataframely as dy
from rich.console import Console
from rich.table import Table

from components.models import Shares
from config import Config
import utils.data
import utils.functions


def get_transfer_coefficient(optimal_shares: dy.DataFrame[Shares], config: Config) -> None:
    # Connect to broker
    broker = config.broker

    # Configure modules
    utils.data.set_config(config=config)

    # Get account value and current (filled) positions
    account_value = broker.get_account_value()
    current_shares = broker.get_positions()

    # Get prices for both portfolios
    tickers = list(
        set(current_shares["ticker"].to_list() + optimal_shares["ticker"].to_list())
    )
    prices = utils.data.get_prices(tickers=tickers)

    # Get benchmark and covariance matrix
    benchmark = utils.data.get_benchmark_weights()
    covariance_matrix = utils.data.get_covariance_matrix(
        tickers=benchmark["ticker"].sort().to_list()
    )

    # Get active weights for optimal and realized portfolios
    active = []
    for shares in (optimal_shares, current_shares):
        dollars = utils.functions.get_dollars(shares=shares, prices=prices)
        weights = utils.functions.get_weights_from_dollars(
            dollars=dollars, account_value=account_value
        )
        _, active_weights = utils.functions.decompose_weights(
            benchmark=benchmark, weights=weights
        )
        active.append(active_weights)

    tc, tc_risk_adj = utils.functions.get_transfer_coefficient(
        optimal_active=active[0],
        realized_active=active[1],
        covariance_matrix=covariance_matrix,
    )

    # Render UI
    table = Table(title="[bold cyan]Transfer Coefficient[/bold cyan]", padding=(0, 2))
    table.add_column("Metric", style="cyan", justify="left")
    table.add_column("Value", style="bold white", justify="right")
    table.add_row("TC (active weights)", f"{tc:.3f}")
    table.add_row("TC (risk-adjusted)", f"{tc_risk_adj:.3f}")
    Console().print(table)

    del broker
    del config.broker
