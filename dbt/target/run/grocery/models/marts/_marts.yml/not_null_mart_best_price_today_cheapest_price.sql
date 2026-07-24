
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cheapest_price
from "grocery"."main_marts"."mart_best_price_today"
where cheapest_price is null



  
  
      
    ) dbt_internal_test