# Pipeline Reference

How `python -m sf_trader --config config.yaml` works, what it reads and writes, and how to fix common problems. For setup and the trading steps, see the [README](../README.md). For limit pricing, refreshes and the basket file format, see [limit_orders.md](limit_orders.md).

## Pipeline steps

| Step | What happens | Module |
|---|---|---|
| 1 | Load and validate the config | `config.py` |
| 2 | Pick the weights date and load the optimal weights | `data.py` |
| 3 | Map holdings to tickers and remove `do_not_trade` names | `__main__.py`, `universe.py` |
| 4 | Stop if TWS has working orders, then read positions and capital | `ibkr.py` |
| 5 | Rescale the weights to sum to 1 and size them at the close | `__main__.py` |
| 6 | Fetch quotes, fix target shares, write `portfolio.csv`, diff against holdings, set limit prices and write `orders.csv` | `ibkr.py`, `orders.py` |
| 7 | Compute metrics and write the charts | `metrics.py`, `charts.py` |

`--refresh` skips steps 2, 3 and 5. It loads the saved `portfolio.csv`, repeats the working-order check, and writes only `orders_refresh.csv`.

### Weights date
The portfolio comes from the `optimal_weights` table, which the nightly job in `sf-data-pipelines-quant` builds. Weights dated D are traded on the next session. The run uses the latest weights date before `trade_date`, so `trade_date: 2026-11-16` (a Monday) uses weights from Friday 2026-11-13.

The run stops if that date is more than 4 calendar days before `trade_date`, or if the weights don't sum to 1 or contain a short position. As in the old optimizer, weights below 0.0001 are dropped, the rest are rescaled to sum to 1, and weights are rounded to 4 decimals. Holdings with no ticker or price in the assets table are dropped with a warning.

### Do-not-trade names
These names are removed from the portfolio and never get an order, the remaining weights are rescaled to sum to 1, and held positions stay as they are. When `orders.capital` is `net_liq`, their market value is subtracted from capital. A holding with no price is valued at 0 with a warning.

### Orders
- Target shares are `weight × capital / mid`, fixed at that run's quotes and saved in `portfolio.csv`, then compared against current holdings.
- Quantities have up to 4 decimals. Contracts report a 0.0001 size increment.
- Holdings outside the portfolio are sold in full.
- Trades below `orders.min_trade_value` are skipped.

### Limit prices
See [limit_orders.md](limit_orders.md).

### Transfer coefficient
`order_metrics.png` shows the correlation between optimal active weights and two sets of holdings, each plain and risk-adjusted:

- **Current holdings:** how close the account is to the target before trading.
- **After orders:** holdings if every order fills. This shows the effect of skipped orders, and it doesn't measure execution quality.

Values use closes on the weights date, and the benchmark is the cap-weighted Russell 3000.

## Configuration
All inputs come from `config.yaml`. Unknown keys and invalid values stop the run at startup.

```yaml
trade_date: 2026-11-02
do_not_trade:
  - MONDQ
  - MOND
  - VELO
  - BGXXQ
  - PSTX.CVR
orders:
  limit_aggressiveness: 0.25
  min_trade_value: 50
  capital: net_liq
  quote_fallback: skip
ibkr:
  host: 127.0.0.1
  port: 7497
  client_id: 8675309
```

| Key | Required | Meaning |
|---|---|---|
| `trade_date` | Yes | The day you are trading. `--trade-date` overrides it |
| `do_not_trade` | No | Tickers that are never traded |
| `orders.limit_aggressiveness` | Yes | `α` in the limit price formula, from 0 to 1 |
| `orders.min_trade_value` | Yes | Smallest trade in dollars |
| `orders.capital` | Yes | `net_liq` or a fixed dollar amount |
| `orders.quote_fallback` | No | `skip` (default) or `last` |
| `orders.refresh_aggressiveness` | No | `α` for `--refresh`, from 0 to 1. Default 0.75. `--aggressiveness` overrides it |
| `ibkr.host`, `ibkr.port`, `ibkr.client_id` | Yes | TWS connection |

## Data
Tables live under `$ROOT/groups/grp_quant/database/$DATABASE/`, and sf-trader only reads them.

### Optimal weights
`optimal_weights/optimal_weights_{year}.parquet`, with one file per year and one row per date and stock. `weight` is the total portfolio weight, benchmark plus active tilt.

### Universe data
Russell 3000 membership comes from monthly FTSE Russell snapshots, which `assets_flow.py` forward-fills by `barrid` into `in_universe`. The FTSE table isn't refreshed by the daily pipeline job. Refreshing it needs a WRDS password and a Duo push, so it's run by hand. See `DEVELOPER.md` in `sf-data-pipelines-quant`.

## Output files

### `portfolio.csv`

| Column | Meaning |
|---|---|
| `ticker`, `barrid` | Identifiers |
| `weight` | Target weight of capital |
| `price` | Close on the weights date |
| `target_shares` | Target position at the run's mids. `--refresh` uses this, and names without a quote keep an estimate at the close |

### `orders.csv` and `orders_refresh.csv`
See [the basket file format](limit_orders.md#basket-file-format).

## Read-only access
`ibkr.py` only requests account summary, positions, open orders, contract details and market data. Open orders are read with `reqAllOpenOrders`, which doesn't bind them to the client. A test fails if any module in `sf_trader` calls an order or cancel method. Ticking **Read-Only API** in TWS adds a second safeguard.

The client doesn't use some `ibapi.sync_wrapper` helpers:

- `get_next_valid_id` repeats ids, which merges responses from separate requests. `IBKRClient` numbers its own requests.
- The wrapper's waits can miss a reply that arrives before they start, so the client polls `response_data` instead.

Market data requests are grouped into batches of 90 to stay under the default line limit. Quotes are live if the account is subscribed and delayed otherwise.

## Troubleshooting

| Message or symptom | Cause | Fix |
|---|---|---|
| `Latest optimal weights are from ...` | Local data is stale, or the nightly job failed | Run `scripts/download_by_year.sh`, and check the nightly `optimal_weights` job if the weights are still old |
| `Optimal weights for <date> are invalid` | The weights don't sum to 1 or have shorts, as on 2026-11-11 to 11-16 | Don't trade. Check the nightly `optimal_weights` job |
| `dropped N holdings with no ticker or price` | The assets table is missing data for a holding | Check the listed barrids before trading |
| `Failed to connect to TWS` | TWS is closed, or the API is off or on another port | Open TWS and check its API settings against `ibkr.port` |
| `No response from TWS` | TWS is connected but not answering | Check that TWS is logged in, then run again |
| Client id already in use | Another API session is connected | Close it or change `ibkr.client_id` |
| `skipped N tickers with no usable quote` | Market closed, or no market data for those names | Run during market hours and check the tickers |
| `orders.csv` has only a header | Every ticker was skipped, or holdings already match targets | Read the run's warnings |
| `No module named 'ibapi'` | A plain `uv sync` removed it | Reinstall from `IBJts` and use `uv sync --inexact` from then on |
| BasketTrader rejects the file | Format problem | Compare with a basket exported from TWS using **Save Orders as Basket** |
| `working orders in TWS` | Orders are still open, and new ones would double up | Cancel them in TWS and run again |
| `tagged [...], not SF_...` | `--refresh` date differs from the day's full run | Use the same `trade_date`, or do a full run |
| `has no target_shares column` | `portfolio.csv` was written before refresh existed | Do a full run |

## Known limitations
As of 2026-11-02:

- **Sizes are being rounded.** BasketTrader accepted the file on 2026-11-02, but fractional quantities were changed to round lots, leaving the paper account about 129% invested. The cause isn't confirmed. See [size rounding](limit_orders.md#warning-size-rounding).
- **Some orders are unexplained.** `EGY`, `KWY`, `LAND` and `UTMD` had no position, working order or completed record that day. Check them in TWS.
- **Limit orders with fractional shares.** IBKR notes that the fractional part of an unmarketable limit order may not execute until it becomes marketable.
- **Delayed quotes.** The paper account has no live market data subscription, so limit prices use delayed quotes unless one is added.
- **`barra_momentum` coverage.** A Barra gap on Jun 22–26, 2026 leaves about 2% of the universe with a value. The fix in `sf-data-pipelines-quant` is on branch `fix/universe-filter-before-scores` and hasn't been deployed.
- **Sub-$5 names in z-scores.** Until that fix is deployed, the pipeline scores these names before sf-trader removes them, which distorts the other names' scores.
- **No concentration limit.** `max_weight` is off by default, and related share classes such as `GOOG` and `GOOGL` count separately.
- **Fill order.** Buys and sells are independent orders. Buys that fill before a large sale can briefly use margin.
