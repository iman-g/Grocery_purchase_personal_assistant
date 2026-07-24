
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select product_id
from "grocery"."main_staging"."stg_ah_products"
where product_id is null



  
  
      
    ) dbt_internal_test