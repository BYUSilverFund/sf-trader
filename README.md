# Silver Fund Trading Repository

Turns the alphas from `sf-data-pipelines-quant` into a target portfolio and a CSV for IBKR's BasketTrader.

**This repo never places orders.** IBKR access is read-only: account value, positions, contract details and quotes. You review `orders.csv` and import it into BasketTrader yourself.

## Setup
1. Install dependencies

```bash
uv sync
```

2. Download [IBKR Trader Work Station (TWS)](https://www.interactivebrokers.com/en/trading/download-tws.php?p=latest) and enable the API. Ticking **Read-Only API** in the TWS API settings is recommended.

3. Download the [IBKR TWS API](https://interactivebrokers.github.io/#), using the "latest" version for synchronous API support. Move the `IBJts` folder to the repository root and install it:

```bash
cd IBJts/source/pythonclient
python setup.py install
```

Run `uv sync --inexact` afterwards, because a plain `uv sync` removes `ibapi`.

4. Add `ROOT` and `DATABASE` to `.env`. Data is read from `$ROOT/groups/grp_quant/database/$DATABASE`.

## Usage
1. Refresh local data from the supercomputer:

```bash
scripts/download_by_year.sh 2026
```

2. Open TWS, set `trade_date` in `config.yaml`, and run:

```bash
python -m sf_trader --config config.yaml
python -m sf_trader --config config.yaml --trade-date 2026-11-03   # override the date
```

3. Check the charts, then import `orders.csv` in BasketTrader.

If active risk isn't about 5%, adjust `constraints.risk_aversion` and run again.

## Pipeline
1. Load and validate the config.
2. Load alphas. Alphas dated D are traded on the next session, so the run uses the latest alpha date before `trade_date` and fails if that date is more than 4 calendar days old.
3. Apply the universe filter: Russell 3000 membership and close above `universe.min_price` on the alpha date, plus a ticker and risk data. Drop counts are printed as warnings.
4. Combine the signals.
5. Remove `do_not_trade` names. They are never traded, held positions stay as they are, and their value is taken out of `net_liq`.
6. Optimize and write `portfolio.csv`.
7. Read positions and quotes from TWS, and diff targets against holdings. Holdings outside the portfolio are sold.
8. Set limit prices and write `orders.csv`.
9. Write `portfolio_metrics.png` and `order_metrics.png`.

## Config

| Key | Meaning |
|---|---|
| `trade_date` | Date you are trading on |
| `signals` | Names from the `signal_name` column of the alpha files: `barra_momentum`, `barra_reversal`, `ivol`, `beta`, `momentum`, `reversal` |
| `combination_strategy` | `equal_weight`. A signal missing for a name counts as 0 |
| `do_not_trade` | Tickers that are never traded |
| `universe.min_price` | Names must close above this |
| `constraints.risk_aversion` | Optimizer gamma. Required, and must be tuned for the decimal alpha scale |
| `constraints.long_only`, `full_investment`, `no_buying_on_margin`, `unit_beta`, `zero_beta`, `zero_investment` | Optional, true or false |
| `constraints.max_weight` | Optional largest weight per name |
| `constraints.weight_decimals` | Weights are rounded to this, and smaller weights are dropped. Default 4 |
| `orders.limit_aggressiveness` | α in the limit price formula, from 0 to 1 |
| `orders.min_trade_value` | Trades below this many dollars are skipped |
| `orders.capital` | `net_liq` or a dollar amount |
| `orders.quote_fallback` | `skip` drops names without a two-sided quote. `last` prices them at last, then close |
| `ibkr.host`, `port`, `client_id` | TWS connection |

## Alpha files
`$ROOT/groups/grp_quant/database/$DATABASE/alphas/alphas_{year}.parquet`, one file per year. From 2025 onward the columns are `date`, `barrid`, `signal_name`, `alpha`. Alphas are in decimal space, and tickers come from joining the assets table on `barrid`. Files from 2023 and 2024 use `alpha_value` and add `ticker`, so scanning across all years fails.

## Outputs

| File | Contents |
|---|---|
| `portfolio.csv` | `ticker, barrid, weight, combined_alpha, price, shares`. `price` is the alpha-date close, and `shares` is an estimate at that price |
| `orders.csv` | BasketTrader import file |
| `portfolio_metrics.png` | Risk, exposure and largest weights |
| `order_metrics.png` | Order totals, skips, transfer coefficient and largest orders |

Limit prices are `mid + α(ask − mid)` for buys and `mid − α(mid − bid)` for sells. Buys round down to the tick and sells round up.

`orders.csv` columns are `Action, Quantity, Symbol, SecType, Exchange, Currency, TimeInForce, OrderType, LmtPrice, BasketTag`, following IBKR's [Basket Fields](https://ibkrguides.com/traderworkstation/basket-fields.htm) and [Manually Create a Basket File](https://ibkrguides.com/traderworkstation/manually-create-a-basket-file.htm). Quantities use up to 4 decimals. Class shares use a space, as in `BRK B`.

The transfer coefficient compares optimal active weights with current holdings and with holdings after all orders fill.

## Tests

```bash
python -m pytest tests
```
