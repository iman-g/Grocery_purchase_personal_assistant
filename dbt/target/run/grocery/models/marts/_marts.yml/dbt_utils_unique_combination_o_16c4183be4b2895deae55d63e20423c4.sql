
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        store, product_id
    from "grocery"."main_marts"."dim_products"
    group by store, product_id
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test