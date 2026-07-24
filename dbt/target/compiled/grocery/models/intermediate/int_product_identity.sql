-- One row per distinct product seen in either store, with an English name where
-- available. AH products get their English name from the translation memory (by id);
-- Lidl products already carry title_eng from the scraper.
--
-- Cross-store product matching (linking an AH product to the "same" Lidl product) is
-- deliberately NOT done here with a fuzzy join in SQL — it belongs in the ML/identity
-- step where rapidfuzz can be applied. This model exposes a clean, English-named
-- product dimension per store for that step (and the app) to build on.

with ah_products as (

    -- One row per AH product_id, using its MOST RECENT scrape values.
    -- ~661 products were renamed across scrape days, so a plain DISTINCT would emit
    -- multiple rows per id and break the product grain. Take the latest instead.
    select
        store,
        product_id,
        product_name_nl,
        main_category,
        sub_category,
        brand
    from (
        select
            store,
            product_id,
            product_name_nl,
            main_category,
            sub_category,
            brand,
            row_number() over (
                partition by product_id
                order by scrape_date desc
            ) as rn
        from "grocery"."main_staging"."stg_ah_products"
    )
    where rn = 1

),

ah_named as (

    select
        p.store,
        p.product_id,
        p.product_name_nl,
        coalesce(t.product_name_en, p.product_name_nl) as product_name_en,
        p.main_category,
        p.sub_category,
        p.brand
    from ah_products p
    left join "grocery"."main_staging"."stg_translation_memory" t
        on t.product_id = p.product_id
       and t.store = 'albert_heijn'

),

lidl_products as (

    -- One row per Lidl product (keyed by name hash), using its latest scrape values.
    select
        store,
        product_id,
        product_name_nl,
        -- fall back to the Dutch name when the scraper had no English translation
        -- (same coalesce logic as the AH side; ~225 Lidl products untranslated)
        coalesce(product_name_en, product_name_nl) as product_name_en,
        cast(null as varchar)      as main_category,
        cast(null as varchar)      as sub_category,
        cast(null as varchar)      as brand
    from (
        select
            store,
            md5(product_name_nl)   as product_id,
            product_name_nl,
            product_name_en,
            row_number() over (
                partition by md5(product_name_nl)
                order by scrape_date desc
            ) as rn
        from "grocery"."main_staging"."stg_lidl_offers"
    )
    where rn = 1

)

select * from ah_named
union all
select * from lidl_products