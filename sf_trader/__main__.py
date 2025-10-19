import click
from pathlib import Path
from rich import print
import datetime as dt
import sf_trader.data_utils as du
import sf_trader.portfolio_utils as pu
import sf_trader.config_utils as cu

@click.command()
@click.option(
    '--config',
    type=click.Path(exists=True, path_type=Path),
    default='config.yml',
    help='Path to configuration file'
)
@click.option(
    '--dry-run',
    is_flag=True,
    help='Simulate trades without executing'
)
def main(config: Path, dry_run: bool):
    """sf-trader: Interactive terminal trading application"""
    trade_date = dt.date(2025, 10, 16)

    # 1. Parse config
    cfg = cu.load_config(config)
    print(cfg)

    # 2. Get universe
    tickers = du.get_tickers(trade_date=trade_date)

    # 3. Get account value
    available_funds = du.get_available_funds()

    # 3. Get prices
    if dry_run:
        prices = du.get_barra_prices(trade_date=trade_date)
    
    # 4. Get tradable universe
    tradable_tickers = pu.get_tradable_tickers(prices)

    # 5. Get data
    lookback_days = min([signal.lookback_days for signal in cfg.signals])
    assets = du.get_asset_data(tickers=tradable_tickers, trade_date=trade_date, lookback_days=lookback_days)

    # 6. Get alphas
    alphas = pu.get_alphas(assets, config=cfg)

    # 7. Get portfolio weights
    weights = pu.get_portfolio_weights(alphas, config=cfg, trade_date=trade_date)

    # 8. Get trades
    trades = pu.get_trades(weights, prices, config=cfg, available_funds=available_funds)

    # 9. Execute trades
    
if __name__ == '__main__':
    main()