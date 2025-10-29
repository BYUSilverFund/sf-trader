from sf_trader.components.signals import get_signal
from sf_trader.components.combinators import get_combinator
from sf_trader.components.constraints import get_constraint

import datetime as dt
import yaml


class ConfigError(Exception):
    """Raised when configuration is invalid"""

    pass


class Config:
    def __init__(self, config_path: str) -> None:
        # Load raw config
        with open(config_path, "r") as f:
            raw_config = yaml.safe_load(f)

        # Parse ignored tickers
        try:
            self.ignore_tickers = raw_config.get("ignore-tickers", [])
            if not isinstance(self.ignore_tickers, list):
                raise ConfigError("'ignore_tickers' must be a list of tickers")
        except ValueError as e:
            raise ConfigError(f"Invalid ignore_tickers configuration: {e}")

        # Parse and build signals
        try:
            signal_names = raw_config.get("signals", [])
            if not isinstance(signal_names, list):
                raise ConfigError("'signals' must be a list of signal names")
            self.signals = [get_signal(name) for name in signal_names]
        except ValueError as e:
            raise ConfigError(f"Invalid signal configuration: {e}")

        # Get ic parameter
        self.ic = float(raw_config.get("ic"))
        if not isinstance(self.ic, (float)):
            raise ConfigError(f"'gamma' must be a float, got {type(self.ic).__name__}")

        # Parse and build signal combinator
        try:
            combinator_name = raw_config.get("signal-combinator", "mean")
            self.signal_combinator = get_combinator(combinator_name)
        except ValueError as e:
            raise ConfigError(f"Invalid signal combinator configuration: {e}")

        # Parse and build constraints
        try:
            constraint_names = raw_config.get("constraints", [])
            if not isinstance(constraint_names, list):
                raise ConfigError("'constraints' must be a list of constraint names")
            self.constraints = [get_constraint(name) for name in constraint_names]
        except ValueError as e:
            raise ConfigError(f"Invalid constraint configuration: {e}")

        # Get gamma parameter
        self.gamma = float(raw_config.get("gamma"))
        if not isinstance(self.gamma, (float)):
            raise ConfigError(
                f"'gamma' must be a number, got {type(self.gamma).__name__}"
            )

        # Get decimal_places parameter
        self.decimal_places = int(raw_config.get("decimal-places"))
        if not isinstance(self.decimal_places, (int)):
            raise ConfigError(
                f"'decimal_places' must be an integer, got {type(self.decimal_places).__name__}"
            )

        self.data_date = dt.date(2025, 10, 21)
