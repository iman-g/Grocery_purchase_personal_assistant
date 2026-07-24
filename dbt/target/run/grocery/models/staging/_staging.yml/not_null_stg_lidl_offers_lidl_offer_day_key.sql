
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select lidl_offer_day_key
from "grocery"."main_staging"."stg_lidl_offers"
where lidl_offer_day_key is null



  
  
      
    ) dbt_internal_test