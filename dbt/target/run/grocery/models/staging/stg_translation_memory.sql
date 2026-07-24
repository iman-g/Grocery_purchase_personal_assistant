
  
  create view "grocery"."main_staging"."stg_translation_memory__dbt_tmp" as (
    -- NL->EN translation cache. Keyed by AH product id for the 'albert_heijn' rows.
-- Used downstream to give AH products an English name and to match across stores.

with source as (

    select * from "grocery"."raw"."raw_translation_memory"

)

select
    nullif(trim(store), '')          as store,
    cast(id as varchar)              as product_id,
    trim(dutch_title)                as product_name_nl,
    nullif(trim(english_title), '')  as product_name_en
from source
where id is not null
  );
