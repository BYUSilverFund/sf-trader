# in_universe investigation (2026-09-21)

## Summary

Filtering to `in_universe=True` does not throw at the load step. The production
assets table has `in_universe` null for every row on the configured data date
(`data-date: 2026-09-10` in `config.yml`), so the filter returns 0 rows and the
optimizer receives an empty universe. The actual traceback has not been seen;
the failure is presumed to come from the empty universe reaching the optimizer.

## Data findings

Source: `/Users/nathanpreslar/groups/grp_quant/database/production/assets/assets_2026.parquet`
(read by `sfd.load_assets_by_date`).

| Period | `in_universe` null rate |
|---|---|
| 2024 – 2026-03-31 | ~0.3% (healthy, ~2,930 True per day) |
| 2026-04-01 → 2026-05-31 | No rows at all (data jumps 03-31 → 06-10, a 71-day gap) |
| 2026-06-10 → 2026-09-03 | ~63% null |
| 2026-09-04 → 2026-09-10 | 100% null, 0 True |

- On 2026-09-10: 23,383 rows, all `in_universe` null.
- Barrids labelled on 2026-02-27 are 0% null through March and ~62% null from
  June, so labels were lost across the Apr–May gap.
- No duplicate (date, barrid) rows.
- Parquet schemas are identical across all years, so this is not a schema mismatch.

## Trader-side behavior

- `sfd.load_assets_by_date(..., in_universe=True)` scans all years and filters
  `pl.col("in_universe")`; nulls are dropped, so the result is empty.
  With `in_universe=False` it scans only `date_.year`.
- `test.py:28,57` and `sf_trader/broker/test.py:17` use `in_universe=True` and
  would produce an empty ticker list.
- `sf_trader/utils/data.py:get_universe` (lines 17-28) and `get_benchmark_weights`
  (lines 98-117) load with `in_universe=False` then `.fill_null(True)`. On
  2026-09-10 this keeps every row, so the optimizer would get ~23k tickers
  instead of ~3k. This silently turns "unknown" into "in the universe".
  Not run, so unconfirmed which path produces the reported error.

## Pipeline repo findings (`../sf-data-pipelines-quant`)

- **FTSE source is stale.** `ftse_russell_2026.parquet` has only two snapshots
  (2026-01-30, 2026-02-27); 2025 has one per month. The assets table shows a
  Russell reconstitution on 2026-06-30 (1,014 R1000 names) that is not in the
  FTSE table, so the two tables came from different runs. FTSE data comes from
  WRDS (`ftse_russell_us.idx_holdings_us`).
- **Assets is backfill-only.** `barra_daily_flow` updates returns, risk and
  volume but not `assets`. The table only changes when `assets_backfill_flow`
  is run.
- **Local checkout is old.** Branch `main` is 8 commits behind origin, with an
  uncommitted debug print and `start=date(2025,1,1)` edit in
  `pipelines/assets_flow.py`.
- **Upstream rewrite.** `fc31565` (2026-04-01) rewrote the assets flow: ticker
  and cusip are now joined separately (LOCALID with the "US" prefix stripped,
  and CUSIP), and rebalance dates come from the FTSE table's own dates rather
  than dates where any asset has Russell membership. Which version produced the
  production files is unknown (all were touched 2026-09-11).
- **Not confirmed:** the exact reason the 2026-03-31 labels did not carry into
  June. Determining it requires re-running the flow.

## Suggested fixes

1. **Pipeline (root cause):**
   - Pull `origin/main`.
   - Refresh the FTSE table (needs WRDS; data may end in Feb 2026).
   - Re-run `assets_backfill_flow` against production.
   - Investigate the Apr–May 2026 gap in the `barra_*` tables.
   - With the new logic, membership after the last FTSE date should
     forward-fill from 2026-02-27, removing nulls but leaving membership stale
     until FTSE data is updated.
2. **Trader (stopgap):**
   - Build `get_universe` from the latest non-null `in_universe` per barrid.
   - Raise a clear error if the universe is empty.
   - Drop the `fill_null(True)` behavior in `get_universe` and
     `get_benchmark_weights`.

No changes were made to either repo during the investigation.
