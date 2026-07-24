





with validation_errors as (

    select
        store, product_id, scrape_date
    from "grocery"."main_marts"."fct_price_daily"
    group by store, product_id, scrape_date
    having count(*) > 1

)

select *
from validation_errors


