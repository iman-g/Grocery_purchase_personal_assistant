
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select store
from "grocery"."main_intermediate"."int_price_history"
where store is null



  
  
      
    ) dbt_internal_test