with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date_add('day',-{days_in_advance},current_date),-- initial date
        date_add('day',-1,current_date),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,order_refund_incidents as (
    select 
        p_creation_date
        ,count(distinct id) count_id_order_refund_incidents
    from delta.contact_order_refund_incidents_odp.order_refund_incidents
    where true
        and p_creation_date in (select calendar_date from calendar_dates)
    group by 1
)

select 
    *
from order_refund_incidents