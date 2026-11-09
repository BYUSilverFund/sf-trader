# Limit Order Pricing and the Basket File

How `sf-trader` prices limit orders, how to refresh orders that stall, and the format of the files uploaded to BasketTrader.

## The tradeoff
Every order balances two costs:

- **Spread cost:** the more aggressive the price, the more of the bid-ask spread you pay.
- **Non-fill cost:** the more passive the price, the more likely the order sits unfilled while the price moves away and the alpha decays.

A market order pays the whole half-spread and sometimes more, while a passive limit order pays nothing but may never fill. `sf-trader` starts in between, then gets more aggressive only for what stays unfilled.

## The pricing rule
`α` is the aggressiveness, from 0 to 1. It sets what fraction of the half-spread you pay beyond the midpoint.

```text
mid        = (bid + ask) / 2
buy limit  = mid + α × (ask − mid)
sell limit = mid − α × (mid − bid)
```

| `α` | Limit price | Behavior |
|---|---|---|
| 0 | Midpoint | Cheapest, least likely to fill |
| 0.25 | A quarter of the way to the far side | Default for the first basket (`orders.limit_aggressiveness`) |
| 0.75 | Three quarters of the way to the far side | Default for a refresh (`orders.refresh_aggressiveness`) |
| 1 | Far side of the quote | Marketable, pays the full half-spread |

An order priced inside the spread rests on the book until the market comes to it. Only at `α = 1` is it immediately marketable, so `α` improves the odds of a fill without guaranteeing one. `sf-trader` doesn't have fill-rate measurements yet, so these defaults are starting points. Tune them from real results.

## Tick rounding
Limit prices have to land on a valid tick, usually $0.01. The direction depends on the file:

| File | Buys | Sells | Why |
|---|---|---|---|
| `orders.csv` | Round down | Round up | Rounding never costs extra |
| `orders_refresh.csv` | Round up | Round down | Rounding moves toward a fill |

This matters most when spreads are tight. With a one-cent spread, the first basket rests on the near side of the quote at any `α` below 1, which is why large caps can stall even when the spread looks cheap. A refresh rounds toward the market, so those orders become marketable and pay the one cent.

## Examples
Delayed quotes from 2026-11-02. "Cost" is the distance from the midpoint per share.

| Quote | Side | First basket (`α = 0.25`) | Cost | Refresh (`α = 0.75`) | Cost |
|---|---|---|---|---|---|
| `SWBI` 13.22 / 13.24 | Buy | 13.23 | $0.00 | 13.24 | $0.01 |
| `AAPL` 337.46 / 337.50 | Buy | 337.48 | $0.00 | 337.50 | $0.02 |
| `AAPL` 337.46 / 337.50 | Sell | 337.48 | $0.00 | 337.46 | $0.02 |
| `BH.A` 1887.36 / 1897.68 | Buy | 1893.81 | $1.29 | 1896.39 | $3.87 |

The wider the spread, the more `α` is worth tuning. `BH.A` shows why a refresh shouldn't simply jump to `α = 1`, which would cost $5.16 a share.

## Missing quotes
A quote needs both a bid and an ask, with the bid at or below the ask. Anything else follows `orders.quote_fallback`:

- `skip` (default) leaves the ticker out and names it in a warning.
- `last` prices it at last, then close, ignoring the spread. Use this only for testing outside market hours.

Quotes are live if the account has a market data subscription and delayed otherwise. With delayed quotes, limit prices are based on an older market, so a price that looks marketable may not be.

## Refreshing stalled orders
Cancelling and repricing is sound when alpha is decaying and the market has moved away. It has three rules:

- **Reprice only the remainder.** Partial fills are already in your holdings, so a refresh diffs against the saved targets.
- **Keep the targets fixed.** A refresh doesn't rerun the optimizer. Rerunning it mid-day would change the portfolio you are trading, not just the prices.
- **Never stack orders.** New orders on top of working ones trade the same shares twice, so `sf-trader` refuses to write a file while any order is working.

`sf-trader` never cancels anything, so you do that step in TWS.

### Steps
1. In TWS, cancel the working orders and wait until none are left.
2. Run the refresh:

```bash
python -m sf_trader --config config.yaml --refresh
python -m sf_trader --config config.yaml --refresh --aggressiveness 0.9   # optional override
```

3. Check the printed remaining buys and sells, then review `orders_refresh.csv`.
4. Upload `orders_refresh.csv` to BasketTrader with the same steps as `orders.csv`.

A refresh uses `portfolio.csv` and `orders.csv` from the day's full run, and it stops if `orders.csv` is tagged for a different `trade_date`. Holdings above target get sell orders. Trades below `orders.min_trade_value` are left alone.

### How often to refresh
- Give orders time to work before cancelling. Each refresh loses your place in the queue, so frequent repricing can reduce fills.
- Raise `α` in steps, such as 0.75 and then 0.9, instead of going straight to 1.
- Compare the new limits with where the name was when the day started. If a stock has run far from your original price, consider leaving it instead of chasing.

### Alternatives to manual repricing
IBKR's basket fields list `REL` as an order type, and there are algo fields such as `Algo Strategy`. These can adjust prices without a cancel and replace, but neither has been tested here.

## Basket file format
`orders.csv` and `orders_refresh.csv` use the same layout. The field names come from IBKR's [Basket Fields](https://ibkrguides.com/traderworkstation/basket-fields.htm) guide.

```csv
Action,Quantity,Symbol,SecType,Exchange,Currency,TimeInForce,OrderType,LmtPrice,BasketTag
BUY,164.8426,AAPL,STK,SMART/NASDAQ,USD,DAY,LMT,344.63,SF_20261102
SELL,10.7488,IWV,STK,SMART/ARCA,USD,DAY,LMT,428.86,SF_20261102_R1318
```

| Column | Type | Allowed values | Notes |
|---|---|---|---|
| `Action` | Text | `BUY`, `SELL` | |
| `Quantity` | Number | Above 0, up to 4 decimals | See the warning on size rounding below |
| `Symbol` | Text | IBKR symbol | Class shares use a space, so `BRK.B` becomes `BRK B` |
| `SecType` | Text | `STK` | |
| `Exchange` | Text | `SMART/<primary>` or `SMART` | Primary exchange comes from TWS contract details |
| `Currency` | Text | `USD` | |
| `TimeInForce` | Text | `DAY` | Unfilled orders expire at the close |
| `OrderType` | Text | `LMT` | |
| `LmtPrice` | Number | Valid tick | Set by the pricing rule |
| `BasketTag` | Text | `SF_<YYYYMMDD>` or `SF_<YYYYMMDD>_R<HHMM>` | The `_R` suffix marks a refresh. TWS shows it as the order's `orderRef` |

File rules from IBKR's [Manually Create a Basket File](https://ibkrguides.com/traderworkstation/manually-create-a-basket-file.htm) guide:

- The first row is the header, and each later row is one order.
- Columns can be in any order, as long as values match their headers.
- There are no spaces around commas, and values aren't quoted.

## Warning: size rounding
**Don't assume `Quantity` is what gets traded.** The first basket on 2026-11-02 had fractional quantities on all 91 orders. TWS accepted the file, but sizes changed before execution:

| Example | `Quantity` in file | Actual size |
|---|---|---|
| `LAUR` | 109.1495 | 100 (working order) |
| `DAN` | 1,252.7346 | 1,300 (held) |
| `GOOGL` | 178.4287 | 160 (held) |
| `IWV` | 2,370.7488 | 2,360 (sold) |

Sizes landed on multiples of 100 for stocks below about $250 and 40 for those from about $250 to $1,000. IBKR's [Transmit a Basket Order](https://ibkrguides.com/traderworkstation/transmit-a-basket-order.htm) guide warns that quantities aren't guaranteed when the basket multiplier isn't 100%, because exchange-required size rounding applies. Whether that setting caused this hasn't been confirmed.

Rounding has two consequences:

- **Exposure can drift.** That day the account ended about 129% invested, with cash of −$296,158, even though the optimizer's weights summed to 1.
- **Refreshes can't fix small differences.** A refresh order smaller than a round lot may be rounded to zero or up to a full lot.

Before relying on these orders, upload a small test basket in paper trading and compare each row's size with `Quantity` before transmitting.
