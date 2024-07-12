with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date({start_date}),
        date({end_date}),
        interval '1' day
    )) as dates (calendar_date)
    where 1=1
)

select
    month(p_creation_date) as year,
    order_vertical as vertical,
    sum(order_total_purchase_eur) as gmv    
from delta.central_order_descriptors_odp.order_descriptors_v2 
where 1=1
    and p_creation_date in (select calendar_date from calendar_dates)
    and order_final_status = 'DeliveredStatus'
group by 1,2