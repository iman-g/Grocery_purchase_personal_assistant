-- Daily price fact, enriched with the English product name and category.
-- One row per store per product per day. This is the table charts and the ML
-- feature layer trend over time.

with prices as (

    select * from "grocery"."main_intermediate"."int_price_history"

),

products as (

    select store, product_id, product_name_en, main_category, sub_category, brand
    from "grocery"."main_marts"."dim_products"

)

select
    p.store,
    p.product_id,
    d.product_name_en,
    d.main_category,
    d.sub_category,
    d.brand,
    p.price,
    p.original_price,
    p.discount_pct,
    p.is_on_offer,
    p.scrape_date
from prices p
left join products d
    on d.store = p.store
   and d.product_id = p.product_id