# dbt project — grocery

Transforms the raw DuckDB warehouse (`../grocery.duckdb`) into tested, documented,
analytics-ready marts. This is the analytics-engineering core of the project.

## Layers

```
raw.*  (built by warehouse/load.py)
  └── staging/        1:1 cleaning, typing, dedupe  (views)
        stg_ah_products, stg_lidl_offers, stg_translation_memory
  └── intermediate/   business logic                (views)
        int_price_history      unified AH+Lidl price time series
        int_product_identity   English-named products per store
  └── marts/          analytics-ready               (tables)
        dim_products           product dimension
        fct_price_daily        daily price fact
        mart_best_price_today  cheapest store per product (the headline table)
```

## Run

```bash
pip install dbt-duckdb
cd dbt
dbt deps                              # installs dbt_utils
dbt build --profiles-dir .            # runs models + tests
dbt docs generate --profiles-dir .    # builds lineage + docs
dbt docs serve --profiles-dir .       # opens the lineage graph in a browser
```

`dbt build` runs models and their tests together and fails loudly if any test breaks —
which is the point.

## Known limitation (by design)

`mart_best_price_today` matches products across stores by exact normalised English name,
which is deliberately conservative — it never wrongly equates two different products, but
it under-matches paraphrases, so only a handful of items pair across AH and Lidl today.
Fuzzy cross-store matching (rapidfuzz) is layered on in the ML step; the exact-match count
here is the baseline it should beat.

## Data-quality note

Some historical scrape days (e.g. 2026-02-22/23) landed partial catalogs. The dbt tests
and source-freshness config are there to catch exactly this kind of gap.
