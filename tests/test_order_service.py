import polars as pl
from polars.testing import assert_frame_equal

from sf_trader.service.order_service import OrderService


class TestOrderService:
    def test_get_order_deltas_basic_buy_and_sell(
        self,
        fake_config,
        portfolio_dao,
        surface_dao,
    ):
        service = OrderService(
            config=fake_config,
            portfolio_dao=portfolio_dao,
            surface_dao=surface_dao,
        )

        prices = pl.DataFrame(
            {
                "ticker": ["AAPL", "MSFT"],
                "price": [200.0, 100.0],
            }
        )

        current_shares = pl.DataFrame(
            {
                "ticker": ["AAPL", "MSFT"],
                "shares": [1.0, 5.0],
            }
        )

        optimal_shares = pl.DataFrame(
            {
                "ticker": ["AAPL", "MSFT"],
                "shares": [3.0, 4.0],
            }
        )

        result = service.get_order_deltas(
            prices=prices,
            current_shares=current_shares,
            optimal_shares=optimal_shares,
        )

        expected = pl.DataFrame(
            {
                "ticker": ["AAPL", "MSFT"],
                "price": [200.0, 100.0],
                "shares": [2.0, 1.0],
                "action": ["BUY", "SELL"],
            }
        )

        assert_frame_equal(result, expected)

    def test_get_order_deltas_fills_missing_positions_with_zero(
        self,
        fake_config,
        portfolio_dao,
        surface_dao,
    ):
        service = OrderService(
            config=fake_config,
            portfolio_dao=portfolio_dao,
            surface_dao=surface_dao,
        )

        prices = pl.DataFrame(
            {
                "ticker": ["AAPL", "MSFT"],
                "price": [200.0, 100.0],
            }
        )

        current_shares = pl.DataFrame(
            {
                "ticker": ["AAPL"],
                "shares": [1.0],
            }
        )

        optimal_shares = pl.DataFrame(
            {
                "ticker": ["MSFT"],
                "shares": [4.0],
            }
        )

        result = service.get_order_deltas(
            prices=prices,
            current_shares=current_shares,
            optimal_shares=optimal_shares,
        )

        expected = pl.DataFrame(
            {
                "ticker": ["AAPL", "MSFT"],
                "price": [200.0, 100.0],
                "shares": [1.0, 4.0],
                "action": ["SELL", "BUY"],
            }
        )

        assert_frame_equal(result, expected)

    def test_get_order_deltas_filters_out_ignored_zero_hold_and_null_price(
        self,
        fake_config,
        portfolio_dao,
        surface_dao,
    ):
        fake_config.ignore_tickers = ["TSLA"]

        service = OrderService(
            config=fake_config,
            portfolio_dao=portfolio_dao,
            surface_dao=surface_dao,
        )

        prices = pl.DataFrame(
            {
                "ticker": ["AAPL", "MSFT", "TSLA", "NVDA"],
                "price": [200.0, 100.0, 300.0, None],
            }
        )

        current_shares = pl.DataFrame(
            {
                "ticker": ["AAPL", "MSFT", "TSLA", "NVDA"],
                "shares": [1.0, 4.0, 0.0, 0.0],
            }
        )

        optimal_shares = pl.DataFrame(
            {
                "ticker": ["AAPL", "MSFT", "TSLA", "NVDA"],
                "shares": [3.0, 4.0, 5.0, 2.0],
            }
        )

        result = service.get_order_deltas(
            prices=prices,
            current_shares=current_shares,
            optimal_shares=optimal_shares,
        )

        expected = pl.DataFrame(
            {
                "ticker": ["AAPL"],
                "price": [200.0],
                "shares": [2.0],
                "action": ["BUY"],
            }
        )

        assert_frame_equal(result, expected)

    def test_get_write_orders_reads_computes_writes_and_returns_orders(
        self,
        fake_config,
        portfolio_dao,
        surface_dao,
    ):
        surface_dao.read_portfolio.return_value = pl.DataFrame(
            {
                "ticker": ["AAPL", "MSFT"],
                "shares": [3.0, 4.0],
            }
        )

        fake_config.broker.get_positions.return_value = pl.DataFrame(
            {
                "ticker": ["AAPL", "MSFT"],
                "shares": [1.0, 5.0],
            }
        )

        portfolio_dao.get_prices_by_date.return_value = pl.DataFrame(
            {
                "ticker": ["AAPL", "MSFT"],
                "price": [200.0, 100.0],
            }
        )

        service = OrderService(
            config=fake_config,
            portfolio_dao=portfolio_dao,
            surface_dao=surface_dao,
        )

        result = service.get_write_orders()

        surface_dao.read_portfolio.assert_called_once()
        fake_config.broker.get_positions.assert_called_once()
        portfolio_dao.get_prices_by_date.assert_called_once()

        called_kwargs = portfolio_dao.get_prices_by_date.call_args.kwargs
        assert called_kwargs["date"] == "2026-03-25"
        assert set(called_kwargs["tickers"]) == {"AAPL", "MSFT"}

        surface_dao.write_orders.assert_called_once()

        written_df = surface_dao.write_orders.call_args.args[0]

        expected = pl.DataFrame(
            {
                "ticker": ["AAPL", "MSFT"],
                "price": [200.0, 100.0],
                "shares": [2.0, 1.0],
                "action": ["BUY", "SELL"],
            }
        )

        assert_frame_equal(result, expected)
        assert_frame_equal(written_df, expected)

    def test_post_orders_reads_orders_and_posts_to_broker(
        self,
        fake_config,
        portfolio_dao,
        surface_dao,
    ):
        orders = pl.DataFrame(
            {
                "ticker": ["AAPL"],
                "price": [200.0],
                "shares": [2.0],
                "action": ["BUY"],
            }
        )

        surface_dao.read_orders.return_value = orders

        service = OrderService(
            config=fake_config,
            portfolio_dao=portfolio_dao,
            surface_dao=surface_dao,
        )

        service.post_orders()

        surface_dao.read_orders.assert_called_once()
        fake_config.broker.post_orders.assert_called_once_with(orders=orders)

    def test_cancel_orders_calls_broker_cancel_orders(
        self,
        fake_config,
        portfolio_dao,
        surface_dao,
    ):
        service = OrderService(
            config=fake_config,
            portfolio_dao=portfolio_dao,
            surface_dao=surface_dao,
        )

        service.cancel_orders()

        fake_config.broker.cancel_orders.assert_called_once()