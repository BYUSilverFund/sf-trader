from pathlib import Path

import dataframely as dy
import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import polars as pl

from sf_trader.metrics import OrderMetrics, PortfolioMetrics
from sf_trader.models import Orders, Portfolio

SURFACE = "#fcfcfb"
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
GRID = "#e4e3df"
SERIES_1 = "#2a78d6"
SERIES_2 = "#eb6834"
TOP_N = 10


def _metrics_table(ax: plt.Axes, title: str, rows: list[tuple[str, str]]) -> None:
    ax.axis("off")
    ax.set_title(title, loc="left", color=TEXT_PRIMARY, fontsize=12, fontweight="bold")
    height = 1 / (len(rows) + 1)
    for i, (label, value) in enumerate(rows):
        y = 1 - (i + 1) * height
        ax.text(0, y, label, color=TEXT_SECONDARY, fontsize=10, transform=ax.transAxes)
        ax.text(
            1, y, value, color=TEXT_PRIMARY, fontsize=10, ha="right", transform=ax.transAxes
        )
        ax.plot([0, 1], [y - height / 3] * 2, color=GRID, lw=1, transform=ax.transAxes)


def _bar_chart(
    ax: plt.Axes,
    title: str,
    labels: list[str],
    values: list[float],
    color: str,
    value_labels: list[str],
) -> None:
    ax.set_title(title, loc="left", color=TEXT_PRIMARY, fontsize=12, fontweight="bold")
    if not labels:
        ax.axis("off")
        ax.text(0, 0.5, "None", color=TEXT_SECONDARY, transform=ax.transAxes)
        return

    positions = [TOP_N - 1 - i for i in range(len(labels))]
    ax.barh(positions, values, color=color, height=0.6)
    ax.set_ylim(-0.5, TOP_N - 0.5)
    ax.set_yticks(positions, labels, color=TEXT_PRIMARY, fontsize=9)
    ax.set_xticks([])
    ax.tick_params(left=False)
    ax.set_xlim(0, max(values) * 1.2)
    for position, value, value_label in zip(positions, values, value_labels):
        ax.text(
            value,
            position,
            f"  {value_label}",
            va="center",
            color=TEXT_SECONDARY,
            fontsize=9,
        )
    for spine in ax.spines.values():
        spine.set_visible(False)


def _new_figure(ncols: int, title: str) -> tuple[plt.Figure, list[plt.Axes]]:
    fig, axes = plt.subplots(1, ncols, figsize=(5 * ncols, 5.5), facecolor=SURFACE)
    for ax in axes:
        ax.set_facecolor(SURFACE)
    fig.suptitle(title, x=0.02, ha="left", color=TEXT_PRIMARY, fontsize=14)
    return fig, list(axes)


def _save(fig: plt.Figure, path: Path) -> None:
    fig.tight_layout()
    fig.savefig(path, dpi=150, facecolor=SURFACE)
    plt.close(fig)


def save_portfolio_chart(
    portfolio: dy.DataFrame[Portfolio],
    metrics: PortfolioMetrics,
    path: Path,
    title: str,
) -> None:
    fig, (table, top_weights) = _new_figure(2, title)

    _metrics_table(
        table,
        "Portfolio metrics",
        [
            ("Active risk", f"{metrics.active_risk:.2%}"),
            ("Total risk", f"{metrics.total_risk:.2%}"),
            ("Gross exposure", f"{metrics.gross_exposure:.2%}"),
            ("Net exposure", f"{metrics.net_exposure:.2%}"),
            ("Positions", f"{metrics.num_positions:,}"),
            ("Dollars allocated", f"${metrics.dollars_allocated:,.0f}"),
            ("Capital", f"${metrics.capital:,.0f}"),
        ],
    )

    top = portfolio.sort("weight", descending=True).head(TOP_N)
    _bar_chart(
        top_weights,
        f"Largest {TOP_N} weights",
        top["ticker"].to_list(),
        top["weight"].to_list(),
        SERIES_1,
        [f"{weight:.2%}" for weight in top["weight"]],
    )

    _save(fig, path)


def save_order_chart(
    orders: dy.DataFrame[Orders], metrics: OrderMetrics, path: Path, title: str
) -> None:
    fig, (table, buys, sells) = _new_figure(3, title)

    _metrics_table(
        table,
        "Order metrics",
        [
            ("Buy orders", f"{metrics.num_buys:,}  (${metrics.buy_value:,.0f})"),
            ("Sell orders", f"{metrics.num_sells:,}  (${metrics.sell_value:,.0f})"),
            ("One-way turnover", f"{metrics.turnover:.2%}"),
            ("Skipped, no quote", f"{metrics.skipped_no_quote:,}"),
            ("Skipped, below minimum", f"{metrics.skipped_small:,}"),
            ("TC, current holdings", f"{metrics.tc_current:.3f}"),
            ("TC risk-adj, current holdings", f"{metrics.tc_current_risk_adjusted:.3f}"),
            ("TC, after orders", f"{metrics.tc_after_orders:.3f}"),
            ("TC risk-adj, after orders", f"{metrics.tc_after_orders_risk_adjusted:.3f}"),
        ],
    )

    for ax, action, color in ((buys, "BUY", SERIES_1), (sells, "SELL", SERIES_2)):
        top = (
            orders.filter(pl.col("action").eq(action))
            .sort("value", descending=True)
            .head(TOP_N)
        )
        _bar_chart(
            ax,
            f"Largest {action.lower()} orders",
            top["ticker"].to_list(),
            top["value"].to_list(),
            color,
            [f"${value:,.0f}" for value in top["value"]],
        )

    _save(fig, path)
