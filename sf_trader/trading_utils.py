from ib_insync import IB, LimitOrder, Stock, util
import polars as pl
import time

def submit_limit_orders(
    trades: pl.DataFrame,
):
    """Submit limit orders for securities in dataframe"""

    # Connect to IBKR
    ib = IB()
    ib.connect("127.0.0.1", 7497, clientId=1)  # Use 7497 for TWS, 4002 for IB Gateway

    results = []

    for trade in trades.to_dicts():
        try:
            ticker = trade["ticker"]
            price = trade["price"]
            quantity = trade["shares"]
            action = "BUY"

            # Calculate limit price (add adjustments here)
            limit_price = round(price + .01, 2)

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

        break

    # Disconnect
    ib.disconnect()

    return pl.DataFrame(results)


# # Monitor order status
# def monitor_orders(ib, order_ids, timeout=300):
#     """Monitor orders until filled or timeout"""
#     start_time = time.time()

#     while time.time() - start_time < timeout:
#         trades = ib.trades()

#         for trade in trades:
#             if trade.order.orderId in order_ids:
#                 print(
#                     f"Order {trade.order.orderId}: {trade.orderStatus.status} - "
#                     f"Filled: {trade.orderStatus.filled}/{trade.order.totalQuantity}"
#                 )

#         # Check if all orders are complete
#         active_orders = [
#             t
#             for t in trades
#             if t.order.orderId in order_ids
#             and t.orderStatus.status in ["PreSubmitted", "Submitted"]
#         ]

#         if not active_orders:
#             print("All orders completed!")
#             break

#         ib.sleep(5)


# # Monitor the orders
# order_ids = results_df[results_df["orderId"].notna()]["orderId"].tolist()
# monitor_orders(ib, order_ids)


# # Cancel all pending orders (if needed)
# def cancel_all_orders(ib):
#     """Cancel all open orders"""
#     for trade in ib.openTrades():
#         ib.cancelOrder(trade.order)
#         print(f"Cancelled order {trade.order.orderId}")

