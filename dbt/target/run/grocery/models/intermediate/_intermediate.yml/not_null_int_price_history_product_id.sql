
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select product_id
from "grocery"."main_intermediate"."int_price_history"
where product_id is null



  
  
      
    ) dbt_internal_test