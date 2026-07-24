
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ah_product_day_key
from "grocery"."main_staging"."stg_ah_products"
where ah_product_day_key is null



  
  
      
    ) dbt_internal_test