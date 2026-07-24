
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select product_name_en
from "grocery"."main_intermediate"."int_product_identity"
where product_name_en is null



  
  
      
    ) dbt_internal_test