with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date_add('day',-{days_in_advance},current_date),-- initial date
        date_add('day',-1,current_date),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,order_descriptors_v2 as (
    select 
        p_creation_date
        ,count(distinct order_id) count_order_id_order_descriptors_v2
    from delta.central_order_descriptors_odp.order_descriptors_v2
    where true
        and p_creation_date in (select calendar_date from calendar_dates)
    group by 1
)

select 
    *
from order_descriptors_v2