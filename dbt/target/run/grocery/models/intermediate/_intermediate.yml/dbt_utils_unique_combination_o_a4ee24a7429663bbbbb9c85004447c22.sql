
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        store, product_id, scrape_date
    from "grocery"."main_intermediate"."int_price_history"
    group by store, product_id, scrape_date
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test