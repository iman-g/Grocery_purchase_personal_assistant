
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select match_name
from "grocery"."main_marts"."mart_best_price_today"
where match_name is null



  
  
      
    ) dbt_internal_test