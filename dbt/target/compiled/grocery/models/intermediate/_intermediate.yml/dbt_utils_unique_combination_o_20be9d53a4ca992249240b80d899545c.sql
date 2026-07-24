





with validation_errors as (

    select
        store, product_id
    from "grocery"."main_intermediate"."int_product_identity"
    group by store, product_id
    having count(*) > 1

)

select *
from validation_errors


