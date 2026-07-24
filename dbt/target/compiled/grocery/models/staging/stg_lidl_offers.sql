-- Lidl daily offers, cleaned and typed.
-- Lidl has no product id, so the (title, scrape_date) pair is the natural key.
-- title_eng is already present from the scraper's translation step.

with source as (

    select * from "grocery"."raw"."raw_lidl_offers"

),

cleaned as (

    select
        trim(title)                                      as product_name_nl,
        nullif(trim(title_eng), '')                      as product_name_en,

        try_cast(price as double)                        as price,
        try_cast(old_price as double)                    as original_price,
        try_cast(discount_percent as double)             as discount_pct,
        nullif(trim(discount_label), '')                 as discount_label,
        nullif(trim(deal_type), '')                      as deal_type,

        -- Lidl Plus (member) pricing vs standard shelf pricing
        lower(coalesce(deal_type, '')) = 'member deal'   as is_member_deal,
        (try_cast(discount_percent as double) > 0)       as is_on_offer,

        nullif(trim(unit), '')                           as unit,
        nullif(trim(url), '')                            as url,
        cast(source_date as date)                        as scrape_date,
        source_file
    from source

)

select
    md5(product_name_nl || '|' || cast(scrape_date as varchar)) as lidl_offer_day_key,
    'lidl'                                               as store,
    *
from cleaned
where price is not null