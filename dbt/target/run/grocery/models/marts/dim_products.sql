
  
    
    

    create  table
      "grocery"."main_marts"."dim_products__dbt_tmp"
  
    as (
      -- Product dimension: one row per product per store, English-named, with category.
-- Grain matches int_product_identity (store + product_id).

select
    store,
    product_id,
    product_name_en,
    product_name_nl,
    main_category,
    sub_category,
    brand
from "grocery"."main_intermediate"."int_product_identity"
    );
  
  