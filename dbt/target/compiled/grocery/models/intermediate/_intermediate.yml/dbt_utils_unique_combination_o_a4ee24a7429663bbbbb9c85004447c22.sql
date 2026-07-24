





with validation_errors as (

    select
        store, product_id, scrape_date
    from "grocery"."main_intermediate"."int_price_history"
    group by store, product_id, scrape_date
    having count(*) > 1

)

select *
from validation_errors


