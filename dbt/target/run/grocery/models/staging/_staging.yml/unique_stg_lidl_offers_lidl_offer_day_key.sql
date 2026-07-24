
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    lidl_offer_day_key as unique_field,
    count(*) as n_records

from "grocery"."main_staging"."stg_lidl_offers"
where lidl_offer_day_key is not null
group by lidl_offer_day_key
having count(*) > 1



  
  
      
    ) dbt_internal_test