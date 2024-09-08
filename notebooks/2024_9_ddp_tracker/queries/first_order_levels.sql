with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date_add('day',-{days_in_advance},current_date),-- initial date
        date_add('day',-1,current_date),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,first_order_levels as (
    select 
        p_order_activated_local_at_date as p_creation_date 
        ,count(distinct order_id) count_count_order_id_first_order_levels
    from delta.mfc__first_order_levels__odp.first_order_levels
    where true
        and p_order_activated_local_at_date in (select calendar_date from calendar_dates)
    group by 1
)

select 
    *
from first_order_levels