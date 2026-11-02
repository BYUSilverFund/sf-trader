# Pipeline Reference

How `python -m sf_trader --config config.yaml` works, what it reads and writes, and how to fix common problems. For setup and the trading steps, see the [README](../README.md).

## Pipeline steps

| Step | What happens | Module |
|---|---|---|
| 1 | Load and validate the config | `config.py` |
| 2 | Pick the alpha date and load alphas | `data.py` |
| 3 | Re-apply the universe filter | `universe.py` |
| 4 | Combine the signals | `combine.py` |
| 5 | Remove `do_not_trade` names | `universe.py` |
| 6 | Read positions and capital from TWS | `ibkr.py` |
| 7 | Optimize and write `portfolio.csv` | `optimize.py` |
| 8 | Fetch quotes, diff against holdings, set limit prices and write `orders.csv` | `ibkr.py`, `orders.py` |
| 9 | Compute metrics and write the charts | `metrics.py`, `charts.py` |

TWS is contacted before the optimizer runs, so a connection problem fails quickly.

### Alpha date
Alphas dated D are traded on the next session, which matches the one-day weight lag in the backtests. The run uses the latest alpha date before `trade_date`. So `trade_date: 2026-11-02` (a Monday) uses alphas from Friday 2026-10-30.

The run stops if that date is more than 4 calendar days before `trade_date`, or if any configured signal has no rows on it.

### Universe filter
The pipeline should already restrict alphas to the tradeable universe. sf-trader checks again so the portfolio can only hold tradeable names. A name is dropped if it fails any of these on the alpha date:

- It isn't in the Russell 3000 (`in_universe` is false, null or missing).
- Its close isn't above `universe.min_price`.
- It has no ticker.
- It has no specific risk, predicted beta or factor exposures.

Drop counts are printed as a warning.

### Do-not-trade names
These names are excluded from the optimizer and never get an order, and held positions stay as they are. When `orders.capital` is `net_liq`, their market value is subtracted from capital. A holding with no price is valued at 0 with a warning.

### Orders
- Target shares are `weight × capital / mid`, compared against current holdings.
- Quantities have up to 4 decimals. Contracts report a 0.0001 size increment.
- Holdings outside the portfolio are sold in full.
- Trades below `orders.min_trade_value` are skipped.

### Limit prices
`α` is `orders.limit_aggressiveness`.

| Side | Limit price | Rounding |
|---|---|---|
| Buy | `mid + α × (ask − mid)` | Down to the tick |
| Sell | `mid − α × (mid − bid)` | Up to the tick |

So `α = 0` is the midpoint and `α = 1` is the far side of the quote.

Missing, one-sided or crossed quotes follow `orders.quote_fallback`:

- `skip` (default) skips the ticker and lists it in a warning.
- `last` prices the ticker at last, then close. Those limits ignore the spread, so use this only for testing outside market hours.

### Transfer coefficient
`order_metrics.png` shows the correlation between optimal active weights and two sets of holdings, each plain and risk-adjusted:

- **Current holdings:** how close the account is to the target before trading.
- **After orders:** holdings if every order fills. This shows the effect of skipped orders, and it doesn't measure execution quality.

Values use alpha-date closes, and the benchmark is the cap-weighted Russell 3000.

## Configuration
All inputs come from `config.yaml`. Unknown keys and invalid values stop the run at startup.

```yaml
trade_date: 2026-11-02
signals:
  - barra_momentum
  - barra_reversal
  - ivol
  - beta
combination_strategy: equal_weight
do_not_trade:
  - MONDQ
  - MOND
  - VELO
  - BGXXQ
  - PSTX.CVR
universe:
  min_price: 5.0
constraints:
  long_only: true
  full_investment: true
  no_buying_on_margin: true
  unit_beta: true
  risk_aversion: 1.5
  weight_decimals: 4
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
| `signals` | Yes | Values from the `signal_name` column of the alpha files |
| `combination_strategy` | Yes | `equal_weight`. A signal missing for a name counts as 0 |
| `do_not_trade` | No | Tickers that are never traded |
| `universe.min_price` | Yes | Names must close above this |
| `constraints.risk_aversion` | Yes | Optimizer gamma |
| `constraints.long_only` | No | No short positions |
| `constraints.full_investment` | No | Weights sum to 1. Final weights are rescaled because the solver meets this only approximately |
| `constraints.no_buying_on_margin` | No | Weights sum to at most 1 |
| `constraints.unit_beta` | No | Portfolio beta of 1 |
| `constraints.zero_beta` | No | Portfolio beta of 0. Can't be combined with `unit_beta` |
| `constraints.zero_investment` | No | Weights sum to 0. Can't be combined with `full_investment` |
| `constraints.max_weight` | No | Largest weight per name, above 0 and up to 1 |
| `constraints.weight_decimals` | No | Weights are rounded to this many decimals, and smaller weights are dropped. Default 4 |
| `orders.limit_aggressiveness` | Yes | `α` in the limit price formula, from 0 to 1 |
| `orders.min_trade_value` | Yes | Smallest trade in dollars |
| `orders.capital` | Yes | `net_liq` or a fixed dollar amount |
| `orders.quote_fallback` | No | `skip` (default) or `last` |
| `ibkr.host`, `ibkr.port`, `ibkr.client_id` | Yes | TWS connection |

### Tuning risk aversion
Alphas are in decimal space, the same as the covariance matrix. Values from older configs, such as `gamma: 100`, don't carry over because those alphas were 100 times larger.

On the 2026-10-30 data:

| `risk_aversion` | Active risk | Positions |
|---|---|---|
| 1.0 | 6.2% | 62 |
| 1.5 | 5.0% | 88 |
| 3.2 | 3.2% | 136 |
| 10 | 1.8% | 281 |
| 100 | 1.2% | 837 |

## Data
Tables live under `$ROOT/groups/grp_quant/database/$DATABASE/`, and sf-trader only reads them.

### Alpha files
`alphas/alphas_{year}.parquet`, with one file per year:

| Years | Columns |
|---|---|
| 2025 onward | `date`, `barrid`, `signal_name`, `alpha` |
| 2023 and 2024 | `date`, `barrid`, `ticker`, `signal_name`, `alpha_value` |

Because the schemas differ, scanning all years at once fails. sf-trader reads one year at a time. Tickers come from joining the `assets` table on `barrid`.

Available signals are `barra_momentum`, `barra_reversal`, `ivol`, `beta`, `momentum` and `reversal`.

### Universe data
Russell 3000 membership comes from monthly FTSE Russell snapshots, which `assets_flow.py` forward-fills by `barrid` into `in_universe`. The FTSE table isn't refreshed by the daily pipeline job. Refreshing it needs a WRDS password and a Duo push, so it's run by hand. See `DEVELOPER.md` in `sf-data-pipelines-quant`.

## Output files

### `portfolio.csv`

| Column | Meaning |
|---|---|
| `ticker`, `barrid` | Identifiers |
| `weight` | Target weight of capital |
| `combined_alpha` | Combined alpha |
| `price` | Close on the alpha date |
| `shares` | Estimate at that close. Orders use live mids instead |

### `orders.csv`
Field names follow IBKR's [Basket Fields](https://ibkrguides.com/traderworkstation/basket-fields.htm) guide.

```csv
Action,Quantity,Symbol,SecType,Exchange,Currency,TimeInForce,OrderType,LmtPrice,BasketTag
BUY,164.8426,AAPL,STK,SMART/NASDAQ,USD,DAY,LMT,344.63,SF_20261102
```

| Column | Value |
|---|---|
| `Action` | `BUY` or `SELL` |
| `Quantity` | Shares, with up to 4 decimals |
| `Symbol` | IBKR symbol. Class shares use a space, so `BRK.B` becomes `BRK B` |
| `SecType` | `STK` |
| `Exchange` | `SMART/<primary exchange>`, or `SMART` if the primary exchange is unknown |
| `Currency` | `USD` |
| `TimeInForce` | `DAY` |
| `OrderType` | `LMT` |
| `LmtPrice` | Limit price |
| `BasketTag` | `SF_<trade date>` |

## Read-only access
`ibkr.py` only requests account summary, positions, contract details and market data. A test fails if any module in `sf_trader` calls an order or cancel method. Ticking **Read-Only API** in TWS adds a second safeguard.

The client doesn't use some `ibapi.sync_wrapper` helpers:

- `get_next_valid_id` repeats ids, which merges responses from separate requests. `IBKRClient` numbers its own requests.
- The wrapper's waits can miss a reply that arrives before they start, so the client polls `response_data` instead.

Market data requests are grouped into batches of 90 to stay under the default line limit. Quotes are live if the account is subscribed and delayed otherwise.

## Troubleshooting

| Message or symptom | Cause | Fix |
|---|---|---|
| `Latest alphas are from ...` | Local data is stale | Run `scripts/download_by_year.sh`, and check the pipeline job if the alphas are still old |
| `No alphas on <date> for [...]` | A signal is missing or misnamed | Compare `signals` with the `Available` list in the message |
| `Failed to connect to TWS` | TWS is closed, or the API is off or on another port | Open TWS and check its API settings against `ibkr.port` |
| `No response from TWS` | TWS is connected but not answering | Check that TWS is logged in, then run again |
| Client id already in use | Another API session is connected | Close it or change `ibkr.client_id` |
| `Optimizer returned no solution` | Constraints can't all be met | Relax the constraints, starting with `max_weight` |
| `skipped N tickers with no usable quote` | Market closed, or no market data for those names | Run during market hours and check the tickers |
| `orders.csv` has only a header | Every ticker was skipped, or holdings already match targets | Read the run's warnings |
| `<signal> covers only N% of the universe` | The signal is missing for many names | Check the pipeline. The run continues and counts missing values as 0 |
| `No module named 'ibapi'` | A plain `uv sync` removed it | Reinstall from `IBJts` and use `uv sync --inexact` from then on |
| BasketTrader rejects the file | Format problem | Compare with a basket exported from TWS, as covered under known limitations |

## Known limitations
As of 2026-11-01:

- **Upload format isn't confirmed.** No `orders.csv` has been uploaded to BasketTrader yet. IBKR's guides don't confirm decimal `Quantity` values, and they show `Smart/NYSE`-style destinations only as an example. Test with a small basket, or export one from TWS with **Save Orders as Basket** and compare the headers.
- **Limit orders with fractional shares.** IBKR notes that the fractional part of an unmarketable limit order may not execute until it becomes marketable.
- **No live quote run yet.** Runs so far were outside market hours, so limit prices have only been tested on made-up quotes and closes.
- **Delayed quotes.** The paper account has no live market data subscription, so limit prices use delayed quotes unless one is added.
- **`barra_momentum` coverage.** A Barra gap on Jun 22–26, 2026 leaves about 2% of the universe with a value. The fix in `sf-data-pipelines-quant` is on branch `fix/universe-filter-before-scores` and hasn't been deployed.
- **Sub-$5 names in z-scores.** Until that fix is deployed, the pipeline scores these names before sf-trader removes them, which distorts the other names' scores.
- **No concentration limit.** `max_weight` is off by default, and related share classes such as `GOOG` and `GOOGL` count separately.
- **Fill order.** Buys and sells are independent orders. Buys that fill before a large sale can briefly use margin.
