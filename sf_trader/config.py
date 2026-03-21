from sf_trader.dal.broker import get_broker

import yaml

_config = None

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
        
        # Get ic parameter
        self.ic = float(raw_config.get("ic"))
        if not isinstance(self.ic, (float)):
            raise ConfigError(f"'ic' must be a float, got {type(self.ic).__name__}")

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

        # Get data date
        try:
            self.data_date = raw_config.get("data-date")
            if not self.data_date:
                raise ConfigError("'data-date' is required")
        except ValueError as e:
            raise ConfigError(f"Invalid data-date format (expected YYYY-MM-DD): {e}")

        # Get broker
        broker_name = raw_config.get("broker")
        self.broker = get_broker(broker_name, self.data_date)


def set_config(config: Config) -> None:
    global _config
    _config = config