with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date_add('day',-{days_in_advance},current_date),-- initial date
        date_add('day',-1,current_date),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,product_collections_flattened_v2 as (
    select 
        loading_date as p_creation_date
        ,count(*) count_product_collections_flattened_v
    from delta.partner_product_availability_odp.product_collections_flattened_v2
    where true
        and loading_date in (select calendar_date from calendar_dates)
    group by 1
)

select 
    *
from product_collections_flattened_v2