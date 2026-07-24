
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select price
from "grocery"."main_staging"."stg_ah_products"
where price is null



  
  
      
    ) dbt_internal_test