-- "Where should we buy this?" — for each product (matched across stores by a
-- normalised English name), the cheapest store on the most recent day it was seen.
--
-- Matching AH<->Lidl on exact normalised name is intentionally conservative: it only
-- pairs items whose English names match after lowercasing/trimming. This under-matches
-- (misses paraphrases) but never wrongly claims two different products are the same,
-- which is the right bias for a "buy the cheaper one" recommendation. Fuzzy matching
-- is layered on later in the ML step.

with latest as (

    -- most recent price per store per product
    select
        store,
        product_id,
        product_name_en,
        main_category,
        price,
        is_on_offer,
        scrape_date,
        row_number() over (
            partition by store, product_id
            order by scrape_date desc
        ) as rn
    from "grocery"."main_marts"."fct_price_daily"
    where product_name_en is not null

),

current_prices as (

    select
        store,
        product_id,
        product_name_en,
        main_category,
        price,
        is_on_offer,
        scrape_date,
        lower(trim(product_name_en)) as match_name
    from latest
    where rn = 1

),

-- rank stores for each normalised product name by price
ranked as (

    select
        *,
        min(price) over (partition by match_name)              as best_price,
        row_number() over (
            partition by match_name order by price asc
        )                                                       as price_rank,
        count(distinct store) over (partition by match_name)    as stores_carrying
    from current_prices

)

select
    match_name,
    product_name_en,
    main_category,
    store              as cheapest_store,
    price              as cheapest_price,
    stores_carrying,
    is_on_offer        as cheapest_is_on_offer,
    scrape_date        as price_as_of
from ranked
where price_rank = 1