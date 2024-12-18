with calendar_dates as(
    select calendar_date
    from unnest(sequence(date({start_date}),date({end_date}), interval '1' day)) as cte (calendar_date)
)

select 
    customer_id,
    count(distinct order_id),
    sum(order_total_purchase_eur)
from delta.central_order_descriptors_odp.order_descriptors_v2 od
inner join calendar_dates cd
    on od.p_creation_date = cd.calendar_date
where true
    and order_city_code = 'BCN'
group by 1