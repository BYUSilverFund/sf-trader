from dataclasses import dataclass


@dataclass
class PortfolioMetrics:
    gross_exposure: float
    net_exposure: float
    num_long: int
    num_short: int
    num_positions: int
    active_risk: float
    total_risk: float
    utilization: float
    account_value: float
    dollars_allocated: float