with calendar_dates as (select
    calendar_date
    from unnest(sequence(
        date({start_date}),
        date({end_date}),
        interval '1' day
    )) as dates (calendar_date)
    where true
)

select 
    od.p_creation_date,
    count(distinct od.order_id) as n_orders,
    count(distinct od.customer_id) as n_customers 
from delta.central_order_descriptors_odp.order_descriptors_v2 od
inner join calendar_dates
    on od.p_creation_date = calendar_dates.calendar_date
where true
group by 1
order by 1,2,3