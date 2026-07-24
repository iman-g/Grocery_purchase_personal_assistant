
    
    

with all_values as (

    select
        store as value_field,
        count(*) as n_records

    from "grocery"."main_intermediate"."int_price_history"
    group by store

)

select *
from all_values
where value_field not in (
    'albert_heijn','lidl'
)


