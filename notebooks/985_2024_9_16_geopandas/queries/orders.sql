with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date({start_date}),-- initial date
        date({end_date}),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,orders as (
    select 
        o.order_id,
        o.order_created_at,
        o.order_city_code,
        dp.point_id,
        dp.latitude,
        dp.longitude
    from delta.central_order_descriptors_odp.order_descriptors_v2 o
    inner join delta.courier_delivery_flow_odp.delivery_points dp
        on o.order_id = dp.order_id
        and o.p_creation_date = dp.p_event_date
    where true
        and p_creation_date in (select calendar_date from calendar_dates)
        and order_country_code in ({country})
)

select 
    *
from orders