with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date_add('day',-{days_in_advance},current_date),-- initial date
        date_add('day',-1,current_date),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,pna_orders_info as (
    select 
        p_creation_date
        ,count(distinct order_id) count_order_id_pna_orders_info
    from delta.mfc__pna__odp.pna_orders_info
    where true
        and p_creation_date in (select calendar_date from calendar_dates)
    group by 1
)

select 
    *
from pna_orders_info