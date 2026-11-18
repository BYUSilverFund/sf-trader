import datetime as dt
from dataclasses import dataclass
from pathlib import Path

import yaml


class ConfigError(Exception):
    """Raised when configuration is invalid"""


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
    do_not_trade: tuple[str, ...]
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

    keys = {"trade_date", "do_not_trade", "orders", "ibkr"}
    if unknown := set(raw) - keys:
        raise ConfigError(f"Unknown top-level keys: {sorted(unknown)}")

    trade_date = trade_date or raw.get("trade_date")
    if type(trade_date) is not dt.date:
        raise ConfigError("'trade_date' must be a date (YYYY-MM-DD)")

    do_not_trade = raw.get("do_not_trade", [])
    if not isinstance(do_not_trade, list):
        raise ConfigError("'do_not_trade' must be a list of tickers")

    return Config(
        trade_date=trade_date,
        do_not_trade=tuple(str(ticker) for ticker in do_not_trade),
        orders=_parse_orders(raw),
        ibkr=_parse_ibkr(raw),
    )


def load_config(path: Path, trade_date: dt.date | None = None) -> Config:
    with open(path, "r") as f:
        return parse_config(yaml.safe_load(f), trade_date=trade_date)
