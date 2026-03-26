import polars as pl
from polars.testing import assert_frame_equal

from sf_trader.service.portfolio_service import PortfolioService


class TestPortfolioService:
    def test_get_optimal_shares_basic(self):
        weights = pl.DataFrame(
            {
                "ticker": ["AAPL", "MSFT"],
                "weight": [0.6, 0.4],
            }
        )

        prices = pl.DataFrame(
            {
                "ticker": ["AAPL", "MSFT"],
                "price": [200.0, 100.0],
            }
        )

        result = PortfolioService.get_optimal_shares(
            weights=weights,
            prices=prices,
            account_value=1000.0,
        )

        expected = pl.DataFrame(
            {
                "ticker": ["AAPL", "MSFT"],
                "shares": [3.0, 4.0],
            }
        )

        assert_frame_equal(result, expected)

    def test_get_optimal_shares_floors_fractional_shares(self):
        weights = pl.DataFrame(
            {
                "ticker": ["AAPL"],
                "weight": [1.0],
            }
        )

        prices = pl.DataFrame(
            {
                "ticker": ["AAPL"],
                "price": [333.33],
            }
        )

        result = PortfolioService.get_optimal_shares(
            weights=weights,
            prices=prices,
            account_value=1000.0,
        )

        expected = pl.DataFrame(
            {
                "ticker": ["AAPL"],
                "shares": [3.0],
            }
        )

        assert_frame_equal(result, expected)

    def test_get_write_portfolio_writes_expected_shares(
        self,
        fake_config,
        portfolio_dao,
        surface_dao,
    ):
        portfolio_dao.get_universe_by_date.return_value = ["AAPL", "MSFT"]

        portfolio_dao.get_prices_by_date.return_value = pl.DataFrame(
            {
                "ticker": ["AAPL", "MSFT"],
                "price": [200.0, 100.0],
            }
        )

        portfolio_dao.get_optimal_weights_by_date.return_value = pl.DataFrame(
            {
                "ticker": ["AAPL", "MSFT"],
                "weight": [0.6, 0.4],
            }
        )

        service = PortfolioService(
            config=fake_config,
            portfolio_dao=portfolio_dao,
            surface_dao=surface_dao,
        )

        service.get_write_portfolio()

        portfolio_dao.get_universe_by_date.assert_called_once_with(
            date="2026-03-25"
        )
        fake_config.broker.get_account_value.assert_called_once()
        portfolio_dao.get_prices_by_date.assert_called_once_with(
            date="2026-03-25",
            tickers=["AAPL", "MSFT"],
        )
        portfolio_dao.get_optimal_weights_by_date.assert_called_once_with(
            date="2026-03-25"
        )

        surface_dao.write_portfolio.assert_called_once()

        written_df = surface_dao.write_portfolio.call_args.args[0]

        expected = pl.DataFrame(
            {
                "ticker": ["AAPL", "MSFT"],
                "shares": [3.0, 4.0],
            }
        )

        assert_frame_equal(written_df, expected)