from types import SimpleNamespace
from unittest.mock import create_autospec

import pytest

from sf_trader.dal.dao.portfolio_dao import PortfolioDAO
from sf_trader.dal.dao.surface_dao import SurfaceDAO


class FakeBroker:
    def get_account_value(self) -> float:
        return 1000.0


@pytest.fixture
def broker():
    return create_autospec(FakeBroker, instance=True, spec_set=True)


@pytest.fixture
def fake_config(broker):
    broker.get_account_value.return_value = 1000.0
    return SimpleNamespace(
        data_date="2026-03-25",
        broker=broker,
    )


@pytest.fixture
def portfolio_dao():
    return create_autospec(PortfolioDAO, instance=True, spec_set=True)


@pytest.fixture
def surface_dao():
    return create_autospec(SurfaceDAO, instance=True, spec_set=True)