with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date_add('day',-{days_in_advance},current_date),-- initial date
        date_add('day',-1,current_date),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,retention_order_info as (
    select 
        p_creation_date
        ,count(*) count_orders_retention_order_info
    from delta.central__retention_orders__odp.retention_order_info
    where true 
        and p_creation_date in (select calendar_date from calendar_dates)
    group by 1
)

select 
    *
from retention_order_info