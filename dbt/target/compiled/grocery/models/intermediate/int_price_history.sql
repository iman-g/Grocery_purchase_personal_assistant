-- Unified price time series across both stores.
-- One row per (store, product, day) with a common schema, so AH and Lidl prices
-- can be compared and trended side by side. This is the backbone the marts and the
-- ML feature layer read from.

with ah as (

    select
        store,
        product_id,
        product_name_nl,
        price,
        original_price,
        discount_pct,
        is_on_offer,
        scrape_date
    from "grocery"."main_staging"."stg_ah_products"

),

lidl as (

    select
        store,
        -- Lidl has no id; use a stable hash of the NL name as its product_id
        md5(product_name_nl)      as product_id,
        product_name_nl,
        price,
        original_price,
        discount_pct,
        is_on_offer,
        scrape_date
    from "grocery"."main_staging"."stg_lidl_offers"

),

unioned as (
    select * from ah
    union all
    select * from lidl
)

select
    store,
    product_id,
    product_name_nl,
    price,
    original_price,
    -- normalise discount depth: prefer explicit pct, else derive from prices
    coalesce(
        discount_pct,
        case
            when original_price is not null and original_price > 0 and price < original_price
            then round(100.0 * (original_price - price) / original_price, 1)
        end
    )                                                    as discount_pct,
    is_on_offer,
    scrape_date
from unioned