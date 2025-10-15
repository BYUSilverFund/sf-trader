import silverfund
import ibkr
from rich import print
import polars as pl

# Note: need to fix ticker mapping

if __name__ == '__main__':
    # ===== Parameters =====
    gamma = 50
    decimal_places = 4

    # ===== Get Tickers =====
    tickers = silverfund.get_tickers()[0:100]

    print(pl.DataFrame({'ticker': tickers}))

    # ===== Get Prices =====
    prices = ibkr.get_prices(tickers=tickers)
    print()
    print("="*60)
    print("Prices")
    print("="*60)
    print(prices)
    print()

    # # ===== Compute Optimal Weights =====
    # weights = silverfund.get_portfolio_weights(gamma=gamma, decimal_places=4)
    # silverfund.create_portfolio_summary(weights)

    # # ===== Get Available Funds =====
    # available_funds = ibkr.get_available_funds()
    # print()
    # print("="*60)
    # print(f"Available Funds:", available_funds)
    # print("="*60)
    # print()

    # # ===== Compute Optimal Shares =====

