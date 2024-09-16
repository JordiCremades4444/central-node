with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date_add('day',-{n_days},current_date),-- initial date
        date_add('day',-1,current_date),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

select 
    p_creation_date,
    count(distinct order_id) as n_orders,
    count(distinct customer_id) as n_customers
from delta.central_order_descriptors_odp.order_descriptors_v2
where 1=1
    and p_creation_date in (select calendar_date from calendar_dates)
group by 1
order by 1 desc