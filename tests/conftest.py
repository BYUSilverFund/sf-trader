from types import SimpleNamespace
from unittest.mock import create_autospec

import polars as pl
import pytest

from sf_trader.dal.dao.portfolio_dao import PortfolioDAO
from sf_trader.dal.dao.surface_dao import SurfaceDAO


class FakeBroker:
    def get_account_value(self) -> float:
        return 1000.0

    def get_positions(self) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "ticker": [],
                "shares": [],
            }
        )

    def post_orders(self, orders: pl.DataFrame) -> None:
        pass

    def cancel_orders(self) -> None:
        pass


@pytest.fixture
def broker():
    return create_autospec(FakeBroker, instance=True, spec_set=True)


@pytest.fixture
def fake_config(broker):
    broker.get_account_value.return_value = 1000.0
    broker.get_positions.return_value = pl.DataFrame(
        {
            "ticker": [],
            "shares": [],
        }
    )

    return SimpleNamespace(
        data_date="2026-03-25",
        broker=broker,
        ignore_tickers=[],
    )


@pytest.fixture
def portfolio_dao():
    return create_autospec(PortfolioDAO, instance=True, spec_set=True)


@pytest.fixture
def surface_dao():
    return create_autospec(SurfaceDAO, instance=True, spec_set=True)