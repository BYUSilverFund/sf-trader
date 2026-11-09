import datetime as dt
from dataclasses import dataclass
from pathlib import Path

import yaml

from sf_trader.combine import STRATEGIES


class ConfigError(Exception):
    """Raised when configuration is invalid"""


@dataclass(frozen=True)
class Constraints:
    risk_aversion: float
    long_only: bool = False
    full_investment: bool = False
    no_buying_on_margin: bool = False
    unit_beta: bool = False
    zero_beta: bool = False
    zero_investment: bool = False
    max_weight: float | None = None
    weight_decimals: int = 4


@dataclass(frozen=True)
class OrderSettings:
    limit_aggressiveness: float
    min_trade_value: float
    capital: str | float
    quote_fallback: str = "skip"
    refresh_aggressiveness: float = 0.75


@dataclass(frozen=True)
class IBKRSettings:
    host: str
    port: int
    client_id: int


@dataclass(frozen=True)
class Config:
    trade_date: dt.date
    signals: tuple[str, ...]
    combination_strategy: str
    do_not_trade: tuple[str, ...]
    min_price: float
    constraints: Constraints
    orders: OrderSettings
    ibkr: IBKRSettings


def _section(raw: dict, name: str, keys: set[str], required: set[str]) -> dict:
    section = raw.get(name)
    if not isinstance(section, dict):
        raise ConfigError(f"'{name}' must be a mapping")
    if unknown := set(section) - keys:
        raise ConfigError(f"Unknown keys in '{name}': {sorted(unknown)}")
    if missing := required - set(section):
        raise ConfigError(f"Missing keys in '{name}': {sorted(missing)}")
    return section


def _number(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"'{name}' must be a number, got {value!r}")
    return float(value)


def _integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"'{name}' must be an integer, got {value!r}")
    return value


def _parse_constraints(raw: dict) -> Constraints:
    flags = {
        "long_only",
        "full_investment",
        "no_buying_on_margin",
        "unit_beta",
        "zero_beta",
        "zero_investment",
    }
    section = _section(
        raw,
        "constraints",
        flags | {"risk_aversion", "max_weight", "weight_decimals"},
        {"risk_aversion"},
    )

    for flag in flags & set(section):
        if not isinstance(section[flag], bool):
            raise ConfigError(f"'constraints.{flag}' must be true or false")

    if section.get("full_investment") and section.get("zero_investment"):
        raise ConfigError("'full_investment' and 'zero_investment' cannot both be true")
    if section.get("unit_beta") and section.get("zero_beta"):
        raise ConfigError("'unit_beta' and 'zero_beta' cannot both be true")

    risk_aversion = _number(section["risk_aversion"], "constraints.risk_aversion")
    if risk_aversion <= 0:
        raise ConfigError("'constraints.risk_aversion' must be positive")

    max_weight = section.get("max_weight")
    if max_weight is not None:
        max_weight = _number(max_weight, "constraints.max_weight")
        if not 0 < max_weight <= 1:
            raise ConfigError("'constraints.max_weight' must be in (0, 1]")

    weight_decimals = _integer(
        section.get("weight_decimals", 4), "constraints.weight_decimals"
    )
    if weight_decimals < 1:
        raise ConfigError("'constraints.weight_decimals' must be at least 1")

    return Constraints(
        risk_aversion=risk_aversion,
        max_weight=max_weight,
        weight_decimals=weight_decimals,
        **{flag: section[flag] for flag in flags & set(section)},
    )


def _parse_orders(raw: dict) -> OrderSettings:
    section = _section(
        raw,
        "orders",
        {
            "limit_aggressiveness",
            "min_trade_value",
            "capital",
            "quote_fallback",
            "refresh_aggressiveness",
        },
        {"limit_aggressiveness", "min_trade_value", "capital"},
    )

    aggressiveness = _number(
        section["limit_aggressiveness"], "orders.limit_aggressiveness"
    )
    if not 0 <= aggressiveness <= 1:
        raise ConfigError("'orders.limit_aggressiveness' must be in [0, 1]")

    refresh_aggressiveness = _number(
        section.get("refresh_aggressiveness", 0.75), "orders.refresh_aggressiveness"
    )
    if not 0 <= refresh_aggressiveness <= 1:
        raise ConfigError("'orders.refresh_aggressiveness' must be in [0, 1]")

    min_trade_value = _number(section["min_trade_value"], "orders.min_trade_value")
    if min_trade_value < 0:
        raise ConfigError("'orders.min_trade_value' cannot be negative")

    capital = section["capital"]
    if capital != "net_liq":
        capital = _number(capital, "orders.capital")
        if capital <= 0:
            raise ConfigError("'orders.capital' must be 'net_liq' or a positive number")

    quote_fallback = section.get("quote_fallback", "skip")
    if quote_fallback not in ("skip", "last"):
        raise ConfigError("'orders.quote_fallback' must be 'skip' or 'last'")

    return OrderSettings(
        limit_aggressiveness=aggressiveness,
        min_trade_value=min_trade_value,
        capital=capital,
        quote_fallback=quote_fallback,
        refresh_aggressiveness=refresh_aggressiveness,
    )


def _parse_ibkr(raw: dict) -> IBKRSettings:
    keys = {"host", "port", "client_id"}
    section = _section(raw, "ibkr", keys, keys)
    if not isinstance(section["host"], str):
        raise ConfigError("'ibkr.host' must be a string")
    return IBKRSettings(
        host=section["host"],
        port=_integer(section["port"], "ibkr.port"),
        client_id=_integer(section["client_id"], "ibkr.client_id"),
    )


def parse_config(raw: dict, trade_date: dt.date | None = None) -> Config:
    if not isinstance(raw, dict):
        raise ConfigError("Config file must contain a mapping")

    keys = {
        "trade_date",
        "signals",
        "combination_strategy",
        "do_not_trade",
        "universe",
        "constraints",
        "orders",
        "ibkr",
    }
    if unknown := set(raw) - keys:
        raise ConfigError(f"Unknown top-level keys: {sorted(unknown)}")

    trade_date = trade_date or raw.get("trade_date")
    if type(trade_date) is not dt.date:
        raise ConfigError("'trade_date' must be a date (YYYY-MM-DD)")

    signals = raw.get("signals")
    if not isinstance(signals, list) or not signals:
        raise ConfigError("'signals' must be a non-empty list of signal names")
    if len(set(signals)) != len(signals):
        raise ConfigError("'signals' contains duplicates")

    strategy = raw.get("combination_strategy")
    if strategy not in STRATEGIES:
        raise ConfigError(
            f"Unknown combination_strategy: {strategy!r}. Available: {sorted(STRATEGIES)}"
        )

    do_not_trade = raw.get("do_not_trade", [])
    if not isinstance(do_not_trade, list):
        raise ConfigError("'do_not_trade' must be a list of tickers")

    universe = _section(raw, "universe", {"min_price"}, {"min_price"})
    min_price = _number(universe["min_price"], "universe.min_price")
    if min_price < 0:
        raise ConfigError("'universe.min_price' cannot be negative")

    return Config(
        trade_date=trade_date,
        signals=tuple(signals),
        combination_strategy=strategy,
        do_not_trade=tuple(str(ticker) for ticker in do_not_trade),
        min_price=min_price,
        constraints=_parse_constraints(raw),
        orders=_parse_orders(raw),
        ibkr=_parse_ibkr(raw),
    )


def load_config(path: Path, trade_date: dt.date | None = None) -> Config:
    with open(path, "r") as f:
        return parse_config(yaml.safe_load(f), trade_date=trade_date)
