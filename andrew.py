import polars as pl

path = "/Users/nathanpreslar/groups/grp_quant/database/production/assets/assets_2026.parquet"

df = pl.scan_parquet(path).filter(pl.col("date") == pl.col("date").max(), pl.col("in_universe")).collect()
print(df)
