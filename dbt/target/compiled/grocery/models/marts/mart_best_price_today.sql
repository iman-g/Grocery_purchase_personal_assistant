-- "Where should we buy this?" — for each product name, the cheapest store and price
-- on the most recent day it was seen.
--
-- Grain: one row per normalised English product name (match_name).
--
-- Matching AH<->Lidl on exact normalised name is intentionally conservative: it only
-- pairs items whose English names match after lowercasing/trimming. It never wrongly
-- claims two different products are the same — the right bias for a "buy the cheaper
-- one" recommendation. Fuzzy matching is layered on later in the ML step.
--
-- Note: within a single store, several distinct product ids can share a normalised
-- name (e.g. "7up regular" in three bottle sizes). We collapse those to the cheapest
-- price per (name, store) first, so the final grain is exactly one row per name.

with latest as (

    -- most recent price per store per product id
    select
        store,
        product_id,
        product_name_en,
        main_category,
        price,
        is_on_offer,
        scrape_date,
        lower(trim(product_name_en)) as match_name,
        row_number() over (
            partition by store, product_id
            order by scrape_date desc
        ) as rn
    from "grocery"."main_marts"."fct_price_daily"
    where product_name_en is not null

),

-- cheapest price per (match_name, store), collapsing same-name product ids
per_store as (

    select
        match_name,
        store,
        min(price)                                as store_price,
        max(main_category)                        as main_category,
        max(product_name_en)                      as product_name_en,
        max(scrape_date)                          as price_as_of,
        bool_or(is_on_offer)                       as is_on_offer
    from latest
    where rn = 1
    group by match_name, store

),

-- rank stores for each name by price
ranked as (

    select
        *,
        row_number() over (
            partition by match_name order by store_price asc
        )                                                    as price_rank,
        count(*) over (partition by match_name)              as stores_carrying
    from per_store

)

select
    match_name,
    product_name_en,
    main_category,
    store              as cheapest_store,
    store_price        as cheapest_price,
    stores_carrying,
    is_on_offer        as cheapest_is_on_offer,
    price_as_of
from ranked
where price_rank = 1