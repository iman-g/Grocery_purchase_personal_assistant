
  
  create view "grocery"."main_staging"."stg_ah_products__dbt_tmp" as (
    -- Albert Heijn daily catalog, cleaned and typed.
-- One row per product per scrape day. Prices cast to numeric; booleans parsed;
-- a surrogate key added so downstream tests can assert uniqueness per (product, day).

with source as (

    select * from "grocery"."raw"."raw_ah_products"

),

cleaned as (

    select
        cast(id as varchar)                              as product_id,
        cast(hq_id as varchar)                           as hq_id,
        trim(title)                                      as product_name_nl,
        nullif(trim(brand), '')                          as brand,
        nullif(trim(main_category), '')                  as main_category,
        nullif(trim(sub_category), '')                   as sub_category,
        nullif(trim(scraped_aisle), '')                  as aisle,

        try_cast(final_price as double)                  as price,
        try_cast(original_price as double)               as original_price,
        try_cast(discount_pct as double)                 as discount_pct,
        nullif(trim(discount_label), '')                 as discount_label,
        try_cast(deal_price as double)                   as deal_price,
        try_cast(deal_count as double)                   as deal_count,

        nullif(trim(unit), '')                           as unit,
        nullif(trim(unit_price_description), '')         as unit_price_description,

        -- is_bonus arrives as the string 'True'/'False'
        lower(coalesce(is_bonus, 'false')) = 'true'      as is_on_offer,
        nullif(trim(discount_type), '')                  as discount_type,
        nullif(trim(bonus_mechanism), '')                as bonus_mechanism,
        try_cast(bonus_start as date)                     as bonus_start,
        try_cast(bonus_end as date)                       as bonus_end,

        nullif(trim(nutriscore), '')                     as nutriscore,
        lower(coalesce(available_online, 'false')) = 'true' as available_online,
        nullif(trim(url), '')                            as url,

        cast(source_date as date)                        as scrape_date,
        source_file
    from source

)

select
    md5(product_id || '|' || cast(scrape_date as varchar)) as ah_product_day_key,
    'albert_heijn'                                       as store,
    *
from cleaned
-- Drop rows with no usable price. Two cases:
--   1. NULL final_price (~527/day) — product with no price returned.
--   2. price = 0 with original_price = 0 (~255 total) — broken API records, mostly
--      multi-packs / wine cases / non-food where AH returned no price. These are not
--      genuinely "free" items; keeping them would pollute price comparisons and the
--      recommender. The dbt accepted_range test on price (> 0) guards this invariant.
where price is not null
  and price > 0
  );
