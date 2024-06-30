with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date({start_date}),-- initial date
        date({end_date}),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

select
    od.order_city_code,
    od.order_country_code,
    count(distinct od.order_id) as n_orders
from delta.central_order_descriptors_odp.order_descriptors_v2 od
left join delta.partner_stores_odp.stores_v2 s
    on od.store_id = s.store_id
    and s.p_end_date is null
where 1=1
    and od.p_creation_date in (select calendar_date from calendar_dates)
    and s.store_subvertical = 'QCPartners'
    and s.store_subvertical2 = 'Groceries'
group by 1,2
order by 3 desc
limit {top}