
    
    

with all_values as (

    select
        is_on_offer as value_field,
        count(*) as n_records

    from "grocery"."main_staging"."stg_ah_products"
    group by is_on_offer

)

select *
from all_values
where value_field not in (
    'True','False'
)


