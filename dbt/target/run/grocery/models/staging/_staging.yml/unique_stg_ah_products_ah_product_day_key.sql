
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    ah_product_day_key as unique_field,
    count(*) as n_records

from "grocery"."main_staging"."stg_ah_products"
where ah_product_day_key is not null
group by ah_product_day_key
having count(*) > 1



  
  
      
    ) dbt_internal_test