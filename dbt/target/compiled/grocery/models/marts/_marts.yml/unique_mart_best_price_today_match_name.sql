
    
    

select
    match_name as unique_field,
    count(*) as n_records

from "grocery"."main_marts"."mart_best_price_today"
where match_name is not null
group by match_name
having count(*) > 1


