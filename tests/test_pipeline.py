import ast
import csv
import datetime as dt
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sf_trader.__main__ import RefreshError, load_saved_portfolio, require_no_working_orders
from sf_trader.charts import save_order_chart, save_portfolio_chart
from sf_trader.combine import combine_alphas
from sf_trader.config import ConfigError, Constraints, OrderSettings, parse_config
from sf_trader.ibkr import from_ibkr_symbol, to_ibkr_symbol
from sf_trader.optimize import MaxWeight, build_constraints, clean_weights
from sf_trader.metrics import (
    OrderMetrics,
    PortfolioMetrics,
    get_transfer_coefficient,
    holdings_after_orders,
)
from sf_trader.models import Orders, Portfolio, Positions, Quotes
from sf_trader.orders import (
    BASKET_COLUMNS,
    compute_orders,
    compute_targets,
    limit_price,
    round_to_tick,
    to_basket,
    write_basket,
)
from sf_trader.universe import apply_do_not_trade, apply_universe_filter

SETTINGS = OrderSettings(limit_aggressiveness=0.25, min_trade_value=50.0, capital="net_liq")


def assets_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "barrid": ["A", "B", "C", "D", "E"],
            "ticker": ["AAA", "BBB", "CCC", None, "EEE"],
            "price": [10.0, 5.0, 20.0, 30.0, 40.0],
            "in_universe": [True, True, False, True, None],
            "predicted_beta": [1.0, 1.0, 1.0, 1.0, 1.0],
            "specific_risk": [20.0, 20.0, 20.0, 20.0, 20.0],
            "market_cap": [1.0, 1.0, 1.0, 1.0, 1.0],
        }
    )


def portfolio_frame(rows: list[tuple[str, float]]) -> pl.DataFrame:
    return Portfolio.validate(
        pl.DataFrame(
            {
                "ticker": [row[0] for row in rows],
                "barrid": [row[0] for row in rows],
                "weight": [row[1] for row in rows],
                "combined_alpha": [0.01] * len(rows),
                "price": [100.0] * len(rows),
                "target_shares": [1.0] * len(rows),
            }
        )
    )


def positions_frame(rows: list[tuple[str, float]]) -> pl.DataFrame:
    return Positions.validate(
        pl.DataFrame(rows, schema={"ticker": pl.String, "shares": pl.Float64}, orient="row")
    )


def quotes_frame(rows: list[dict]) -> pl.DataFrame:
    base = dict.fromkeys(("bid", "ask", "last", "close"))
    base |= {"min_tick": 0.01, "primary_exchange": "NYSE"}
    return Quotes.validate(
        pl.DataFrame([base | row for row in rows], schema=Quotes.to_polars_schema())
    )


def raw_config() -> dict:
    return {
        "trade_date": dt.date(2026, 11, 2),
        "signals": ["ivol", "beta"],
        "combination_strategy": "equal_weight",
        "do_not_trade": ["AAA"],
        "universe": {"min_price": 5.0},
        "constraints": {"long_only": True, "risk_aversion": 1.0},
        "orders": {"limit_aggressiveness": 0.25, "min_trade_value": 50, "capital": "net_liq"},
        "ibkr": {"host": "127.0.0.1", "port": 7497, "client_id": 1},
    }


class TestConfig:
    def test_valid(self):
        config = parse_config(raw_config())
        assert config.constraints.long_only and config.orders.quote_fallback == "skip"

    def test_trade_date_override(self):
        date = dt.date(2026, 11, 3)
        assert parse_config(raw_config(), trade_date=date).trade_date == date

    @pytest.mark.parametrize(
        "change",
        [
            {"trade_date": "soon"},
            {"signals": []},
            {"combination_strategy": "magic"},
            {"unknown": 1},
            {"orders": {"limit_aggressiveness": 2, "min_trade_value": 50, "capital": "net_liq"}},
            {"orders": {"limit_aggressiveness": 0.2, "min_trade_value": 50, "capital": "cash"}},
            {"orders": {"limit_aggressiveness": 0.2, "min_trade_value": 50, "capital": "net_liq", "refresh_aggressiveness": 2}},
            {"constraints": {"risk_aversion": -1}},
            {"constraints": {"risk_aversion": 1, "typo": True}},
            {"constraints": {"risk_aversion": 1, "unit_beta": True, "zero_beta": True}},
        ],
    )
    def test_invalid(self, change):
        with pytest.raises(ConfigError):
            parse_config(raw_config() | change)


class TestUniverse:
    def test_keeps_only_tradeable_names(self):
        alphas = pl.DataFrame(
            {"barrid": ["A", "B", "C", "D", "E", "Z"], "ivol": [0.1] * 6}
        )
        filtered, dropped = apply_universe_filter(
            alphas, assets_frame(), 5.0, ["A", "B", "C", "D", "E"]
        )
        assert filtered["barrid"].to_list() == ["A"]
        assert dropped == {
            "not_in_russell_3000": 3,  # C is out, E is null, Z is not in assets
            "price_at_or_below_min": 1,  # B sits exactly at the minimum
            "no_ticker": 1,
            "no_risk_data": 0,
        }

    def test_drops_names_without_exposures(self):
        alphas = pl.DataFrame({"barrid": ["A"], "ivol": [0.1]})
        filtered, dropped = apply_universe_filter(alphas, assets_frame(), 5.0, [])
        assert filtered.is_empty() and dropped["no_risk_data"] == 1

    def test_do_not_trade_is_excluded(self):
        alphas = pl.DataFrame({"ticker": ["AAA", "BBB"]})
        remaining, excluded = apply_do_not_trade(alphas, ["AAA", "NOT_THERE"])
        assert remaining["ticker"].to_list() == ["BBB"] and excluded == 1


class TestCombine:
    def test_equal_weight(self):
        alphas = pl.DataFrame({"a": [0.2], "b": [0.4]})
        result = combine_alphas(alphas, ["a", "b"], "equal_weight")
        assert result["combined_alpha"].to_list() == pytest.approx([0.3])

    def test_missing_signal_counts_as_zero(self):
        alphas = pl.DataFrame({"a": [None, 0.4], "b": [0.2, 0.4]}, schema={"a": pl.Float64, "b": pl.Float64})
        result = combine_alphas(alphas, ["a", "b"], "equal_weight")
        assert result["combined_alpha"].to_list() == pytest.approx([0.1, 0.4])


class TestCleanWeights:
    def weights(self) -> pl.DataFrame:
        return pl.DataFrame({"barrid": ["A", "B", "C"], "weight": [0.6, 0.41, 0.00001]})

    def test_full_investment_restores_the_budget(self):
        constraints = Constraints(risk_aversion=1.0, full_investment=True)
        cleaned = clean_weights(self.weights(), constraints)
        assert cleaned["barrid"].to_list() == ["A", "B"]
        assert cleaned["weight"].sum() == pytest.approx(1.0, abs=1e-4)

    def test_without_full_investment_only_rounds(self):
        cleaned = clean_weights(self.weights(), Constraints(risk_aversion=1.0))
        assert cleaned["weight"].to_list() == [0.6, 0.41]

    def test_max_weight_constraint_is_built(self):
        built = build_constraints(Constraints(risk_aversion=1.0, long_only=True, max_weight=0.02))
        assert MaxWeight(0.02) in built and len(built) == 2


class TestOrders:
    def run(self, portfolio, positions, quotes, settings=SETTINGS, capital=10_000.0, do_not_trade=()):
        targets = compute_targets(portfolio, quotes, settings, capital)
        return compute_orders(targets, positions, quotes, settings, list(do_not_trade))

    def test_fractional_diff(self):
        orders, skipped, small = self.run(
            portfolio_frame([("AAA", 0.5), ("BBB", 0.5)]),
            positions_frame([("AAA", 12.3456), ("OLD", 2.5)]),
            quotes_frame(
                [
                    {"ticker": "AAA", "bid": 99.99, "ask": 100.01},
                    {"ticker": "BBB", "bid": 299.99, "ask": 300.01},
                    {"ticker": "OLD", "bid": 49.99, "ask": 50.01},
                ]
            ),
        )
        by_ticker = {row["ticker"]: row for row in orders.iter_rows(named=True)}
        assert by_ticker["AAA"]["action"] == "BUY"
        assert by_ticker["AAA"]["shares"] == pytest.approx(37.6544)
        assert by_ticker["BBB"]["shares"] == pytest.approx(16.6667)
        assert by_ticker["OLD"]["action"] == "SELL"
        assert by_ticker["OLD"]["shares"] == pytest.approx(2.5)
        assert skipped == [] and small == 0

    def test_exit_sells_the_exact_fractional_position(self):
        orders, _, _ = self.run(
            portfolio_frame([("AAA", 1.0)]),
            positions_frame([("IWV", 2370.7488)]),
            quotes_frame(
                [
                    {"ticker": "AAA", "bid": 99.99, "ask": 100.01},
                    {"ticker": "IWV", "bid": 399.0, "ask": 401.0},
                ]
            ),
        )
        row = orders.filter(pl.col("ticker").eq("IWV")).row(0, named=True)
        assert (row["action"], row["shares"]) == ("SELL", 2370.7488)

    def test_min_trade_value(self):
        orders, _, small = self.run(
            portfolio_frame([("AAA", 0.5)]),
            positions_frame([("AAA", 49.8)]),
            quotes_frame([{"ticker": "AAA", "bid": 99.99, "ask": 100.01}]),
        )
        assert orders.is_empty() and small == 1

    def test_do_not_trade_never_gets_an_order(self):
        orders, skipped, _ = self.run(
            portfolio_frame([("AAA", 1.0)]),
            positions_frame([("MOND", 1.0)]),
            quotes_frame([{"ticker": "AAA", "bid": 99.99, "ask": 100.01}]),
            do_not_trade=["MOND"],
        )
        assert orders["ticker"].to_list() == ["AAA"] and skipped == []

    @pytest.mark.parametrize(
        "quote",
        [
            {"last": 100.0, "close": 99.0},  # market closed
            {"bid": 100.0, "last": 100.0},  # one-sided
            {"bid": 101.0, "ask": 100.0, "last": 100.0},  # crossed
        ],
    )
    def test_missing_quote_skips_by_default(self, quote):
        orders, skipped, _ = self.run(
            portfolio_frame([("AAA", 1.0)]),
            positions_frame([]),
            quotes_frame([{"ticker": "AAA"} | quote]),
        )
        assert orders.is_empty() and skipped == ["AAA"]

    def test_last_fallback_uses_last_then_close(self):
        settings = OrderSettings(0.25, 50.0, "net_liq", quote_fallback="last")
        orders, skipped, _ = self.run(
            portfolio_frame([("AAA", 0.5), ("BBB", 0.5)]),
            positions_frame([]),
            quotes_frame(
                [{"ticker": "AAA", "last": 100.0, "close": 99.0}, {"ticker": "BBB", "close": 50.0}]
            ),
            settings=settings,
        )
        assert orders["limit_price"].to_list() == [100.0, 50.0] and skipped == []


class TestRefresh:
    def quotes(self) -> pl.DataFrame:
        return quotes_frame(
            [
                {"ticker": "AAA", "bid": 100.00, "ask": 100.40},
                {"ticker": "BBB", "bid": 50.00, "ask": 50.01},
            ]
        )

    def portfolio(self) -> pl.DataFrame:
        frame = portfolio_frame([("AAA", 0.5), ("BBB", 0.5)])
        return Portfolio.validate(frame.with_columns(pl.Series("target_shares", [100.0, 200.0])))

    def test_only_the_unfilled_remainder_is_ordered(self):
        orders, _, _ = compute_orders(
            self.portfolio(),
            positions_frame([("AAA", 60.0), ("BBB", 200.0)]),
            self.quotes(),
            SETTINGS,
            [],
            toward_market=True,
        )
        assert orders["ticker"].to_list() == ["AAA"]
        assert orders["shares"].to_list() == [40.0]

    def test_targets_stay_fixed_when_prices_move(self):
        moved = self.quotes().with_columns(pl.col("bid", "ask").mul(1.1))
        orders, _, _ = compute_orders(
            self.portfolio(),
            positions_frame([("AAA", 100.0), ("BBB", 200.0)]),
            moved,
            SETTINGS,
            [],
            toward_market=True,
        )
        assert orders.is_empty()

    def test_overfilled_names_are_sold_back(self):
        orders, _, _ = compute_orders(
            self.portfolio(),
            positions_frame([("AAA", 110.0), ("BBB", 200.0)]),
            self.quotes(),
            SETTINGS,
            [],
            toward_market=True,
        )
        assert (orders["action"][0], orders["shares"][0]) == ("SELL", 10.0)

    def test_one_tick_spread_becomes_marketable(self):
        orders, _, _ = compute_orders(
            self.portfolio(),
            positions_frame([("AAA", 100.0)]),
            self.quotes(),
            SETTINGS,
            [],
            toward_market=True,
        )
        assert orders["limit_price"].to_list() == [50.01]

    def test_held_name_at_target_needs_no_quote(self):
        _, skipped, _ = compute_orders(
            portfolio_frame([("AAA", 1.0)]),
            positions_frame([("AAA", 1.0)]),
            quotes_frame([{"ticker": "AAA"}]),
            SETTINGS,
            [],
        )
        assert skipped == []


class TestLimitPrice:
    def test_buy_pays_fraction_of_half_spread(self):
        assert limit_price("BUY", 100.00, 100.40, 0.25, 0.01) == 100.25

    def test_sell_gives_fraction_of_half_spread(self):
        assert limit_price("SELL", 100.00, 100.40, 0.25, 0.01) == 100.15

    def test_zero_aggressiveness_is_mid(self):
        assert limit_price("BUY", 100.00, 100.02, 0.0, 0.01) == 100.01

    def test_full_aggressiveness_reaches_the_touch(self):
        assert limit_price("BUY", 100.00, 100.40, 1.0, 0.01) == 100.40
        assert limit_price("SELL", 100.00, 100.40, 1.0, 0.01) == 100.00

    def test_rounds_in_the_favorable_direction(self):
        assert round_to_tick(100.019, 0.01, "BUY") == 100.01
        assert round_to_tick(100.011, 0.01, "SELL") == 100.02

    def test_on_tick_prices_stay_put(self):
        assert round_to_tick(100.07, 0.01, "BUY") == 100.07
        assert round_to_tick(100.07, 0.01, "SELL") == 100.07

    def test_toward_market_flips_the_rounding(self):
        assert round_to_tick(100.011, 0.01, "BUY", toward_market=True) == 100.02
        assert round_to_tick(100.019, 0.01, "SELL", toward_market=True) == 100.01

    def test_one_tick_spread_rests_on_the_near_touch_by_default(self):
        assert limit_price("BUY", 100.00, 100.01, 0.75, 0.01) == 100.00
        assert limit_price("SELL", 100.00, 100.01, 0.75, 0.01) == 100.01

    def test_one_tick_spread_crosses_toward_market(self):
        assert limit_price("BUY", 100.00, 100.01, 0.75, 0.01, toward_market=True) == 100.01
        assert limit_price("SELL", 100.00, 100.01, 0.75, 0.01, toward_market=True) == 100.00

    def test_sub_penny_tick(self):
        assert round_to_tick(0.51237, 0.0001, "BUY") == 0.5123


class TestBasket:
    def orders(self) -> pl.DataFrame:
        return Orders.validate(
            pl.DataFrame(
                {
                    "ticker": ["AAPL", "BRK.B"],
                    "action": ["BUY", "SELL"],
                    "shares": [12.3456, 3.0],
                    "limit_price": [227.41, 539.8],
                    "value": [2800.0, 1600.0],
                    "primary_exchange": ["NASDAQ", None],
                }
            )
        )

    def test_rows(self):
        assert to_basket(self.orders(), "SF_20261102").rows(named=True) == [
            {
                "Action": "BUY",
                "Quantity": "12.3456",
                "Symbol": "AAPL",
                "SecType": "STK",
                "Exchange": "SMART/NASDAQ",
                "Currency": "USD",
                "TimeInForce": "DAY",
                "OrderType": "LMT",
                "LmtPrice": "227.41",
                "BasketTag": "SF_20261102",
            },
            {
                "Action": "SELL",
                "Quantity": "3",
                "Symbol": "BRK B",
                "SecType": "STK",
                "Exchange": "SMART",
                "Currency": "USD",
                "TimeInForce": "DAY",
                "OrderType": "LMT",
                "LmtPrice": "539.8",
                "BasketTag": "SF_20261102",
            },
        ]

    def test_file_has_no_spaces_around_commas_and_no_quotes(self, tmp_path):
        path = tmp_path / "orders.csv"
        write_basket(self.orders(), path, "TAG")
        text = path.read_text()
        assert text.splitlines()[0] == ",".join(BASKET_COLUMNS)
        assert ", " not in text and " ," not in text and '"' not in text
        with open(path) as f:
            assert all(len(row) == len(BASKET_COLUMNS) for row in csv.reader(f))


class TestSymbols:
    @pytest.mark.parametrize(
        "ticker, symbol",
        [("AAPL", "AAPL"), ("BRK.B", "BRK B"), ("MOG.A", "MOG A"), ("PSTX.CVR", "PSTX.CVR")],
    )
    def test_to_ibkr(self, ticker, symbol):
        assert to_ibkr_symbol(ticker) == symbol

    def test_round_trip(self):
        assert from_ibkr_symbol("BRK B") == "BRK.B"


class TestTransferCoefficient:
    def test_identical_portfolios(self):
        weights = np.array([0.5, -0.2, -0.3])
        tc, adjusted = get_transfer_coefficient(weights, weights, np.eye(3))
        assert (tc, adjusted) == pytest.approx((1.0, 1.0))

    def test_opposite_portfolios(self):
        weights = np.array([0.5, -0.2, -0.3])
        tc, _ = get_transfer_coefficient(weights, -weights, np.eye(3))
        assert tc == pytest.approx(-1.0)

    def test_holdings_after_orders(self):
        positions = positions_frame([("AAA", 10.5), ("BBB", 2.0)])
        orders = Orders.validate(
            pl.DataFrame(
                {
                    "ticker": ["AAA", "CCC"],
                    "action": ["SELL", "BUY"],
                    "shares": [0.5, 3.0],
                    "limit_price": [1.0, 1.0],
                    "value": [1.0, 1.0],
                    "primary_exchange": [None, None],
                },
                schema_overrides={"primary_exchange": pl.String},
            )
        )
        after = dict(holdings_after_orders(positions, orders).iter_rows())
        assert after == {"AAA": 10.0, "BBB": 2.0, "CCC": 3.0}


@pytest.mark.parametrize("action", ["BUY", "SELL"])
def test_charts_save(tmp_path, action):
    portfolio = portfolio_frame([("AAA", 0.6), ("BBB", 0.4)])
    orders = Orders.validate(
        pl.DataFrame(
            {
                "ticker": ["AAA"],
                "action": [action],
                "shares": [1.5],
                "limit_price": [10.0],
                "value": [15.0],
                "primary_exchange": ["NYSE"],
            }
        )
    )
    portfolio_metrics = PortfolioMetrics(0.05, 0.16, 1.0, 1.0, 2, 1e6, 1e6)
    order_metrics = OrderMetrics(1, 0, 15.0, 0.0, 0.1, 0, 0, 0.1, 0.1, 0.9, 0.9)

    save_portfolio_chart(portfolio, portfolio_metrics, tmp_path / "portfolio.png", "Test")
    save_order_chart(orders, order_metrics, tmp_path / "orders.png", "Test")

    assert (tmp_path / "portfolio.png").stat().st_size > 0
    assert (tmp_path / "orders.png").stat().st_size > 0


def test_ibkr_module_makes_no_write_calls():
    """Read-only guard: the client may only use an allow-list of TWS calls."""
    forbidden = {
        "placeOrder",
        "place_order_sync",
        "cancelOrder",
        "cancel_order_sync",
        "reqGlobalCancel",
        "exerciseOptions",
    }
    source = Path("sf_trader/ibkr.py").read_text()
    attributes = {
        node.attr for node in ast.walk(ast.parse(source)) if isinstance(node, ast.Attribute)
    }
    assert not attributes & forbidden
    for path in Path("sf_trader").glob("*.py"):
        assert not {
            node.attr
            for node in ast.walk(ast.parse(path.read_text()))
            if isinstance(node, ast.Attribute)
        } & forbidden, path


class TestLoadSavedPortfolio:
    def write(self, tmp_path, monkeypatch, tag="SF_20261102", shares_column="target_shares"):
        monkeypatch.chdir(tmp_path)
        portfolio_frame([("AAA", 1.0)]).rename({"target_shares": shares_column}).write_csv("portfolio.csv")
        pl.DataFrame({"BasketTag": [tag]}).write_csv("orders.csv")

    def test_loads_matching_run(self, tmp_path, monkeypatch):
        self.write(tmp_path, monkeypatch)
        assert load_saved_portfolio(parse_config(raw_config())).height == 1

    def test_rejects_another_days_orders(self, tmp_path, monkeypatch):
        self.write(tmp_path, monkeypatch, tag="SF_20261030")
        with pytest.raises(RefreshError, match="tagged"):
            load_saved_portfolio(parse_config(raw_config()))

    def test_rejects_estimated_shares(self, tmp_path, monkeypatch):
        self.write(tmp_path, monkeypatch, shares_column="shares")
        with pytest.raises(RefreshError, match="target_shares"):
            load_saved_portfolio(parse_config(raw_config()))

    def test_requires_a_full_run_first(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        with pytest.raises(RefreshError, match="full run"):
            load_saved_portfolio(parse_config(raw_config()))


def test_working_orders_block_new_files():
    class Client:
        def get_working_order_tickers(self):
            return ["LAUR"]

    with pytest.raises(RefreshError, match="LAUR"):
        require_no_working_orders(Client())
