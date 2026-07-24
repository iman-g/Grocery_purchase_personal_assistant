
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        is_member_deal as value_field,
        count(*) as n_records

    from "grocery"."main_staging"."stg_lidl_offers"
    group by is_member_deal

)

select *
from all_values
where value_field not in (
    'True','False'
)



  
  
      
    ) dbt_internal_test