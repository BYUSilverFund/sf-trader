from dataclasses import dataclass
from pathlib import Path
import yaml
from sf_trader.models import Signal, SignalCombinator
from sf_trader.silverfund._signals import get_signal
from sf_trader.silverfund._combinators import get_combinator
from sf_trader.silverfund._constraints import get_constraint
from sf_quant.optimizer.constraints import Constraint

class ConfigError(Exception):
    """Raised when configuration is invalid"""
    pass


@dataclass
class TradingConfig:
    """Trading system configuration"""
    
    # Signals
    signals: list[Signal]
    signal_combinator: SignalCombinator
    
    # Constraints
    constraints: list[Constraint]
    
    # Risk parameters
    gamma: float = 500.0


def load_config(config_path: Path) -> TradingConfig:
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
        with open(config_path, 'r') as f:
            raw_config = yaml.safe_load(f)

    except FileNotFoundError:
        raise ConfigError(f"Configuration file not found: {config_path}")
    
    except yaml.YAMLError as e:
        raise ConfigError(f"Failed to parse YAML: {e}")
    
    except Exception as e:
        raise ConfigError(f"Failed to load configuration: {e}")
    
    
    if not raw_config:
        raise ConfigError("Configuration file is empty")

    # Parse and build signals
    try:
        signal_names = raw_config.get('signals', [])
        if not isinstance(signal_names, list):
            raise ConfigError("'signals' must be a list of signal names")
        signals = [get_signal(name) for name in signal_names]
    except ValueError as e:
        raise ConfigError(f"Invalid signal configuration: {e}")

    # Parse and build signal combinator
    try:
        combinator_name = raw_config.get('signal-combinator', 'mean')
        signal_combinator = get_combinator(combinator_name)
    except ValueError as e:
        raise ConfigError(f"Invalid signal combinator configuration: {e}")

    # Parse and build constraints
    try:
        constraint_names = raw_config.get('constraints', [])
        if not isinstance(constraint_names, list):
            raise ConfigError("'constraints' must be a list of constraint names")
        constraints = [get_constraint(name) for name in constraint_names]
    except ValueError as e:
        raise ConfigError(f"Invalid constraint configuration: {e}")

    # Get gamma parameter
    gamma = raw_config.get('gamma')
    if not isinstance(gamma, (int, float)):
        raise ConfigError(f"'gamma' must be a number, got {type(gamma).__name__}")

    # Build and return config
    try:
        return TradingConfig(
            signals=signals,
            signal_combinator=signal_combinator,
            constraints=constraints,
            gamma=float(gamma)
        )
    except Exception as e:
        raise ConfigError(f"Failed to build configuration: {e}")