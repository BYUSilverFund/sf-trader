from pathlib import Path

import yaml
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from sf_trader.components.combinators import get_combinator
from sf_trader.components.constraints import get_constraint
from sf_trader.components.models import Config
from sf_trader.components.signals import get_signal


class ConfigError(Exception):
    """Raised when configuration is invalid"""

    pass


def load_config(config_path: Path) -> Config:
    """
    Load and parse configuration from YAML file

    Args:
        config_path: Path to YAML configuration file

    Returns:
        TradingConfig object

    Raises:
        ConfigError: If configuration is invalid or cannot be loaded
    """
    # Open config file.
    try:
        with open(config_path, "r") as f:
            raw_config = yaml.safe_load(f)

    except FileNotFoundError:
        raise ConfigError(f"Configuration file not found: {config_path}")

    except yaml.YAMLError as e:
        raise ConfigError(f"Failed to parse YAML: {e}")

    except Exception as e:
        raise ConfigError(f"Failed to load configuration: {e}")

    if not raw_config:
        raise ConfigError("Configuration file is empty")

    # Parse ignored tickers
    try:
        ignore_tickers = raw_config.get("ignore-tickers", [])
        if not isinstance(ignore_tickers, list):
            raise ConfigError("'ignore_tickers' must be a list of tickers")
    except ValueError as e:
        raise ConfigError(f"Invalid ignore_tickers configuration: {e}")

    # Parse and build signals
    try:
        signal_names = raw_config.get("signals", [])
        if not isinstance(signal_names, list):
            raise ConfigError("'signals' must be a list of signal names")
        signals = [get_signal(name) for name in signal_names]
    except ValueError as e:
        raise ConfigError(f"Invalid signal configuration: {e}")

    # Get ic parameter
    ic = raw_config.get("ic")
    if not isinstance(ic, (float)):
        raise ConfigError(f"'gamma' must be a float, got {type(ic).__name__}")

    # Parse and build signal combinator
    try:
        combinator_name = raw_config.get("signal-combinator", "mean")
        signal_combinator = get_combinator(combinator_name)
    except ValueError as e:
        raise ConfigError(f"Invalid signal combinator configuration: {e}")

    # Parse and build constraints
    try:
        constraint_names = raw_config.get("constraints", [])
        if not isinstance(constraint_names, list):
            raise ConfigError("'constraints' must be a list of constraint names")
        constraints = [get_constraint(name) for name in constraint_names]
    except ValueError as e:
        raise ConfigError(f"Invalid constraint configuration: {e}")

    # Get gamma parameter
    gamma = raw_config.get("gamma")
    if not isinstance(gamma, (int, float)):
        raise ConfigError(f"'gamma' must be a number, got {type(gamma).__name__}")

    # Get decimal_places parameter
    decimal_places = raw_config.get("decimal-places")
    if not isinstance(decimal_places, (int)):
        raise ConfigError(
            f"'decimal_places' must be an integer, got {type(decimal_places).__name__}"
        )

    # Build and return config
    try:
        return Config(
            ignore_tickers=ignore_tickers,
            signals=signals,
            ic=float(ic),
            signal_combinator=signal_combinator,
            constraints=constraints,
            gamma=float(gamma),
            decimal_places=int(decimal_places),
        )
    except Exception as e:
        raise ConfigError(f"Failed to build configuration: {e}")


def print_config(cfg: Config, console: Console) -> None:
    """Display the trading configuration in a formatted table."""
    # Universe section
    universe_table = Table(show_header=False, box=None, padding=(0, 2))
    universe_table.add_column("Ticker", style="cyan")
    for ticker in cfg.ignore_tickers:
        universe_table.add_row(f"- {ticker}")

    # Signals section
    signals_table = Table(show_header=False, box=None, padding=(0, 2))
    signals_table.add_column("Signal", style="cyan")
    for signal in cfg.signals:
        signals_table.add_row(f"- {signal.name}")

    # Optimization parameters
    params_table = Table(show_header=False, box=None, padding=(0, 2))
    params_table.add_column("Parameter", style="cyan")
    params_table.add_column("Value", style="bold white")

    params_table.add_row("Signal Combinator", cfg.signal_combinator.name)
    params_table.add_row("Information Coefficient", f"{cfg.ic:.2%}")
    params_table.add_row("Risk Aversion (γ)", f"{cfg.gamma:,.0f}")
    params_table.add_row("Decimal Places", str(cfg.decimal_places))

    # Constraints section
    constraints_table = Table(show_header=False, box=None, padding=(0, 2))
    constraints_table.add_column("Constraint", style="cyan")

    for constraint in cfg.constraints:
        constraints_table.add_row(f"- {constraint.__class__.__name__}")

    console.print(
        Panel(
            universe_table,
            title="[bold cyan]Ignore Tickers[/bold cyan]",
            border_style="cyan",
        )
    )
    console.print(
        Panel(
            signals_table, title="[bold cyan]Signals[/bold cyan]", border_style="cyan"
        )
    )
    console.print(
        Panel(
            params_table,
            title="[bold cyan]Optimization Parameters[/bold cyan]",
            border_style="cyan",
        )
    )
    console.print(
        Panel(
            constraints_table,
            title="[bold cyan]Constraints[/bold cyan]",
            border_style="cyan",
        )
    )
