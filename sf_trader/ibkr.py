from ib_insync import IB, Stock
from typing import Dict
import polars as pl

def get_prices(tickers: list[str], exchange: str = 'SMART', currency: str = 'USD') -> Dict[str, float]:
    """
    Get last prices for a list of tickers (up to 3000) using delayed market data.

    Args:
        tickers: List of ticker symbols
        exchange: Exchange to use (default: 'SMART')
        currency: Currency (default: 'USD')

    Returns:
        Dictionary mapping ticker symbols to their last prices
    """
    ib = IB()

    try:
        # Connect to TWS or IB Gateway
        ib.connect('127.0.0.1', 7497, clientId=1)

        # Switch to delayed market data (no subscription required)
        ib.reqMarketDataType(3)  # 3 = delayed data, 4 = delayed-frozen

        prices = []
        batch_size = 50  # IBKR recommends limiting concurrent requests

        # Process tickers in batches
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]

            # Create contract objects for this batch
            contracts = [Stock(ticker, exchange, currency) for ticker in batch]

            # Qualify contracts (resolve any ambiguities)
            qualified_contracts = ib.qualifyContracts(*contracts)

            # Request market data snapshots for qualified contracts
            for contract in qualified_contracts:
                ticker_obj = ib.reqMktData(contract, snapshot=True)

            # Wait for all tickers in batch to receive data
            ib.sleep(5)  # Give time for snapshots to arrive

            # Extract prices from ticker objects
            for contract in qualified_contracts:
                ticker_obj = ib.ticker(contract)
                symbol = contract.symbol

                # Get the last price (or close price as fallback)
                if ticker_obj.last and ticker_obj.last > 0:
                    prices.append({'ticker': symbol, 'price': ticker_obj.last})
                elif ticker_obj.close and ticker_obj.close > 0:
                    prices.append({'ticker': symbol, 'price': ticker_obj.close})
                else:
                    print(f"Warning: Could not get price for {symbol}")
                    prices.append({'ticker': symbol, 'price':None})

        return pl.DataFrame(prices)

    except Exception as e:
        print(f"Error getting prices: {e}")
        raise
    finally:
        ib.disconnect()

def get_available_funds():
    # Create an IB instance
    ib = IB()

    # Connect to TWS or IB Gateway
    # Default ports: TWS live=7496, TWS paper=7497, Gateway live=4001, Gateway paper=4002
    ib.connect('127.0.0.1', 7497, clientId=1)

    # Get account values
    account_values = ib.accountValues()

    total_cash_value = 0
    available_funds = 0

    for value in account_values:
        match value.tag:
            case 'TotalCashValue':
                total_cash_value = value.value
            
            case 'AvailableFunds':
                available_funds = value.value
            
            case _:
                pass

    ib.disconnect()

    return available_funds
