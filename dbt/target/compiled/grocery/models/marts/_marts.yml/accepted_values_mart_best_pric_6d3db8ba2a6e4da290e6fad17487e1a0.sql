
    
    

with all_values as (

    select
        cheapest_store as value_field,
        count(*) as n_records

    from "grocery"."main_marts"."mart_best_price_today"
    group by cheapest_store

)

select *
from all_values
where value_field not in (
    'albert_heijn','lidl'
)


