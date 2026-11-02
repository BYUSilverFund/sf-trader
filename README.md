# Silver Fund Trading Repository

`sf-trader` turns the alphas from `sf-data-pipelines-quant` into a target portfolio and a CSV of limit orders for IBKR's BasketTrader.

**This repo never places orders.** It only reads account value, positions, contract details and quotes from TWS. You review `orders.csv` and upload it to BasketTrader yourself.

For how the pipeline works, the full config reference and troubleshooting, see [docs/pipeline.md](docs/pipeline.md). For limit pricing, refreshing orders and the basket file format, see [docs/limit_orders.md](docs/limit_orders.md).

## Setup
You only need to do this once.

1. Install dependencies:

```bash
uv sync
source .venv/bin/activate
```

2. Download [IBKR Trader Workstation (TWS)](https://www.interactivebrokers.com/en/trading/download-tws.php?p=latest), using the "latest" version.

3. Download the [IBKR TWS API](https://interactivebrokers.github.io/#), also the "latest" version. Move the `IBJts` folder to the repository root and install it into the virtual environment:

```bash
cd IBJts/source/pythonclient
python setup.py install
cd ../../..
```

`ibapi` isn't in `uv.lock`, so a plain `uv sync` removes it. After this step, use `uv sync --inexact`.

4. Create a `.env` file in the repository root. Data is read from `$ROOT/groups/grp_quant/database/$DATABASE`.

```bash
ROOT="/Users/your-name"
DATABASE="production"
```

5. In TWS, enable the API in the API settings. Ticking **Read-Only API** is recommended as a second safeguard. The port must match `ibkr.port` in `config.yaml`.

## Trading
Run these steps from the repository root with the virtual environment active.

### 1. Refresh the data
This copies the current year's files from the supercomputer. It needs your supercomputer login, and the username and paths are set at the top of the script.

```bash
scripts/download_by_year.sh 2026
```

### 2. Open TWS
Log in to the account you are trading. Run during market hours, because orders need two-sided quotes.

### 3. Set the trade date
Set `trade_date` in `config.yaml` to the day you are trading. The run uses the latest alphas dated before that day. It stops if they are more than 4 calendar days old.

```yaml
trade_date: 2026-11-02
```

You can also override the date on the command line in the next step.

### 4. Run the pipeline

```bash
python -m sf_trader --config config.yaml
python -m sf_trader --config config.yaml --trade-date 2026-11-03   # optional override
```

A run writes four files to the repository root:

| File | Contents |
|---|---|
| `portfolio.csv` | Target weights, combined alpha, alpha-date close and target shares |
| `orders.csv` | The BasketTrader upload file |
| `portfolio_metrics.png` | Risk, exposure and largest weights |
| `order_metrics.png` | Order totals, skipped orders, transfer coefficient and largest orders |

### 5. Check the results
Read the warnings printed by the run, then open both charts.

- **Active risk** should be about 5%. If it isn't, adjust `constraints.risk_aversion` in `config.yaml` and run again. A higher value lowers active risk.
- **Skipped, no quote** should be 0. Anything skipped won't be traded, and the affected tickers are printed in the run's warnings.
- **`orders.csv`** should have one row per order. A file with only a header means no orders were generated, usually because the market is closed.

### 6. Upload to BasketTrader
1. In TWS, open BasketTrader. In Classic TWS it's under **Trading Tools**, and the menu location can differ between TWS versions.
2. Click **Browse**.
3. Select `orders.csv` in the repository root and click **Open**.
4. Check that the order count, sides, quantities and limit prices match the run's output.

Uploading only loads the orders into BasketTrader. Review them there, because transmitting the basket sends live orders to the market.

The steps follow IBKR's guide, [Upload a File to BasketTrader](https://ibkrguides.com/traderworkstation/upload-a-file-to-baskettrader.htm).

**Compare each row's size with `Quantity` before transmitting.** TWS has rounded fractional quantities to round lots, which left the account over-invested. See [size rounding](docs/limit_orders.md#warning-size-rounding).

### 7. Refresh stalled orders
If orders stall, reprice what's still unfilled. `sf-trader` can't cancel orders, so do that part in TWS.

1. Cancel the working orders in TWS, and wait until none are left.
2. Run the refresh, which stops if any order is still working:

```bash
python -m sf_trader --config config.yaml --refresh
python -m sf_trader --config config.yaml --refresh --aggressiveness 0.9   # optional override
```

3. Upload `orders_refresh.csv` the same way as step 6.

A refresh keeps the targets from the day's full run in `portfolio.csv` and doesn't rerun the optimizer. Run it during market hours, because it needs two-sided quotes.

## Tests

```bash
python -m pytest tests
```
