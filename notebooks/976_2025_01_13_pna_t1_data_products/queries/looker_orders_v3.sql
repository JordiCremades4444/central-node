with calendar_dates as (
    select calendar_date
    from unnest(sequence(date({start_date}), date({end_date}), interval '1' day)) as cte (calendar_date)
)

-- Careful orders appear more than once in the table
,orders_with_pna_instructions as (
    select distinct
        oi.orderid as order_id
    from delta.tech__partner_order_analytics_order_dispatched_with_pna_v0__odp.partner_orders_orderdispatchedtopartnerwithpnaanalyticsevent oi
    inner join calendar_dates
        on oi.p_ingestion_date = calendar_dates.calendar_date
    where true
)

select 
    od.store_name,
    od.store_address_id,
    count(distinct od.order_id) as n_orders
from delta.central_order_descriptors_odp.order_descriptors_v2 od
inner join calendar_dates
    on date(od.order_activated_local_at) = calendar_dates.calendar_date
left join orders_with_pna_instructions
    on orders_with_pna_instructions.order_id = od.order_id
where true
    and od.store_address_id in ({sad})
group by 1, 2
order by 1,2
