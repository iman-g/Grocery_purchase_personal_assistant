
    
    

with all_values as (

    select
        store as value_field,
        count(*) as n_records

    from "grocery"."main_marts"."fct_price_daily"
    group by store

)

select *
from all_values
where value_field not in (
    'albert_heijn','lidl'
)


