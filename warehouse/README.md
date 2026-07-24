# Warehouse (DuckDB)

`load.py` builds `grocery.duckdb` — the analytical warehouse that dbt models sit on top of.

It lands **raw** source data into a `raw` schema, one table per source:

| Table | Source | Rows (initial backfill) |
|---|---|---|
| `raw.raw_ah_products` | `ah_full_export1_*.csv` (14 daily snapshots) | ~301k |
| `raw.raw_ah_summary` | `ah_summary1_*.csv` | ~330 |
| `raw.raw_lidl_offers` | `lidl_offers_*.csv` (14 days) | ~1.1k |
| `raw.raw_translation_memory` | `product_translation_memory.csv` | ~24k |
| `raw.raw_receipts` | Google Sheet `Raw` tab (with `--with-receipts`) | your purchases |

Every dated CSV gets a `source_date` and `source_file` column so daily scrapes become a
price **time series**. Cleaning and typing happen downstream in dbt — this layer stays raw.

## Run

```bash
pip install duckdb pandas gspread
python warehouse/load.py                  # loads all CSVs (AH, Lidl, translation memory)
python warehouse/load.py --with-receipts  # also pulls purchase history from the Sheet
```

Then inspect:

```bash
duckdb grocery.duckdb
D SELECT source_date, count(*) FROM raw.raw_ah_products GROUP BY 1 ORDER BY 1;
```

The build is idempotent — re-running rebuilds the raw tables from whatever CSVs are on disk.
`grocery.duckdb` is gitignored (rebuild it locally, don't commit it).
