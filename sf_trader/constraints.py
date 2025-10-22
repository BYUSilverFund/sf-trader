import sf_quant.optimizer as sfo


def get_constraint(name: str) -> sfo.constraints.Constraint:
    """
    Get a constraint by name

    Args:
        name: Name of the constraint (e.g., 'full-investment', 'long-only',
              'no-buying-on-margin', 'unit-beta')

    Returns:
        Constraint instance

    Raises:
        ValueError: If constraint name is not found
    """
    # Map constraint names to constraint constructors/instances
    CONSTRAINTS = {
        "full-investment": sfo.FullInvestment,
        "long-only": sfo.LongOnly,
        "no-buying-on-margin": sfo.NoBuyingOnMargin,
        "unit-beta": sfo.UnitBeta,
    }

    if name not in CONSTRAINTS:
        raise ValueError(
            f"Unknown constraint: {name}. Available: {list(CONSTRAINTS.keys())}"
        )

    return CONSTRAINTS[name]()
