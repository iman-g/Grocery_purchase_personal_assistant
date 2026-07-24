
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select scrape_date
from "grocery"."main_staging"."stg_ah_products"
where scrape_date is null



  
  
      
    ) dbt_internal_test