import yaml
import datetime as dt

from sf_trader.dal.broker import get_broker

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

        # Get decimal_places parameter
        self.decimal_places = int(raw_config.get("decimal-places"))
        if not isinstance(self.decimal_places, (int)):
            raise ConfigError(
                f"'decimal_places' must be an integer, got {type(self.decimal_places).__name__}"
            )

        # Get data date
        try:
            data_date_raw = raw_config.get("data-date")
            if not data_date_raw:
                raise ConfigError("'data-date' is required")
            self.data_date = dt.datetime.strptime(data_date_raw, "%Y-%m-%d").date()
        except ValueError as e:
            raise ConfigError(f"Invalid data-date format (expected YYYY-MM-DD): {e}")

        # Get broker
        broker_name = raw_config.get("broker")
        self.broker = get_broker(broker_name, self.data_date)


def set_config(config: Config) -> None:
    global _config
    _config = config