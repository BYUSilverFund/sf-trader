from ib_insync import IB, LimitOrder, Stock
import polars as pl
from sf_trader.models import Config
import dataframely as dy
from models import Shares, Prices


def get_trades(
    current_shares: dy.DataFrame[Shares],
    optimal_shares: dy.DataFrame[Shares],
    prices: dy.DataFrame[Prices],
    config: Config,
) -> pl.DataFrame:
    tickers = list(
        set(current_shares["ticker"].to_list() + optimal_shares["ticker"].to_list())
    )

    current_shares = current_shares.rename({"shares": "current_shares"})
    optimal_shares = optimal_shares.rename({"shares": "optimal_shares"})

    return (
        pl.DataFrame({"ticker": tickers})
        .join(current_shares, on="ticker", how="left")
        .join(optimal_shares, on="ticker", how="left")
        .join(prices, on="ticker", how="left")
        .with_columns(pl.col("current_shares", "optimal_shares").fill_null(0))
        .with_columns(pl.col("optimal_shares").sub("current_shares").alias("shares"))
        .with_columns(
            pl.when(pl.col("shares").gt(0))
            .then(pl.lit("BUY"))
            .when(pl.col("shares").lt(0))
            .then(pl.lit("SELL"))
            .otherwise(pl.lit("HOLD"))
            .alias("action")
        )
        .with_columns(pl.col("shares").abs())
        .select("ticker", "price", "shares", "action")
        .filter(
            pl.col("ticker").is_in(config.ignore_tickers).not_(),
            pl.col("shares").ne(0),
            pl.col("action").ne("HOLD"),
            pl.col("price").is_not_null(),
        )
        .sort("ticker")
    )


def submit_limit_orders(
    trades: pl.DataFrame,
) -> pl.DataFrame:
    """Submit limit orders for securities in dataframe"""

    # Connect to IBKR
    ib = IB()
    ib.connect("127.0.0.1", 7497, clientId=1)  # Use 7497 for TWS, 4002 for IB Gateway

    results = []

    for trade in trades.to_dicts():
        try:
            ticker = trade["ticker"].replace(".", " ")
            price = trade["price"]
            quantity = trade["shares"]
            action = trade["action"]

            # Calculate limit price (add adjustments here)
            limit_price = round(price + 0.01, 2)

            # Create contract
            contract = Stock(ticker, "SMART", "USD")

            # Create limit order
            order = LimitOrder(
                action=action,
                totalQuantity=quantity,
                lmtPrice=limit_price,
            )

            # Place order
            trade = ib.placeOrder(contract, order)

            # Wait for order acknowledgment
            ib.sleep(0.5)

            results.append(
                {
                    "ticker": ticker,
                    "orderId": trade.order.orderId,
                    "status": trade.orderStatus.status,
                    "action": action,
                    "quantity": quantity,
                    "limit_price": limit_price,
                    "filled": trade.orderStatus.filled,
                    "remaining": trade.orderStatus.remaining,
                    "error": None,
                }
            )

            print(
                f"✓ {ticker}: Order {trade.order.orderId} - {action} {quantity} @ ${limit_price}"
            )

        except Exception as e:
            results.append(
                {
                    "ticker": ticker,
                    "orderId": None,
                    "status": "ERROR",
                    "action": action,
                    "quantity": quantity,
                    "limit_price": None,
                    "filled": 0,
                    "remaining": 0,
                    "error": str(e),
                }
            )
            print(f"✗ {ticker}: Error - {str(e)}")

    # Disconnect
    ib.disconnect()

    return pl.DataFrame(results)


def clear_ibkr_orders() -> pl.DataFrame:
    """Cancel all open orders in IBKR account

    Returns:
        DataFrame with order cancellation results containing:
        - orderId: The order ID that was cancelled
        - ticker: The ticker symbol
        - status: Cancellation status
        - error: Error message if any
    """
    # Connect to IBKR
    ib = IB()
    ib.connect("127.0.0.1", 7497, clientId=1)  # Use 7497 for TWS, 4002 for IB Gateway

    # Get all open trades (includes order, contract, and status)
    open_trades = ib.openTrades()

    results = []

    if not open_trades:
        print("No open orders to cancel")
        ib.disconnect()
        return pl.DataFrame({"orderId": [], "ticker": [], "status": [], "error": []})

    print(f"Found {len(open_trades)} open order(s) to cancel")

    for trade in open_trades:
        try:
            order_id = trade.order.orderId
            ticker = trade.contract.symbol

            # Cancel the order
            ib.cancelOrder(trade.order)

            # Wait for cancellation acknowledgment
            ib.sleep(0.5)

            results.append(
                {
                    "orderId": order_id,
                    "ticker": ticker,
                    "status": "CANCELLED",
                    "error": None,
                }
            )

            print(f"✓ Cancelled order {order_id} for {ticker}")

        except Exception as e:
            results.append(
                {
                    "orderId": order_id if "order_id" in locals() else None,
                    "ticker": ticker if "ticker" in locals() else "UNKNOWN",
                    "status": "ERROR",
                    "error": str(e),
                }
            )
            print(f"✗ Error cancelling order: {str(e)}")

    # Disconnect
    ib.disconnect()

    return pl.DataFrame(results)
