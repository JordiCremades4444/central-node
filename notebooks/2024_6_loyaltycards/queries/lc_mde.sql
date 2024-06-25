with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date({start_date}),-- initial date
        date({end_date}),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,customers_with_at_least_one_loyalty_card as (
    select distinct 
        customer_id
    from delta.mfc__temp_order_loyalty_cards__odp.temp_order_loyalty_cards_enriched
    where 1=1
        and p_creation_date in (select calendar_date from calendar_dates)
        and loyalty_card is not null
)

,customer_dates as (
    select
        c.customer_id,
        d.calendar_date
    from customers_with_at_least_one_loyalty_card c
    cross join calendar_dates d
)

select
    cd.customer_id,
    cd.calendar_date as p_creation_date,
    coalesce(count(distinct od.order_id), 0) as n_orders,
    coalesce(count(distinct lce.loyalty_card), 0) as n_orders_with_loyalty
from customer_dates cd
left join delta.central_order_descriptors_odp.order_descriptors_v2 od
    on cd.customer_id = od.customer_id
    and cd.calendar_date = od.p_creation_date
left join delta.mfc__temp_order_loyalty_cards__odp.temp_order_loyalty_cards_enriched lce
    on cd.customer_id = lce.customer_id
    and cd.calendar_date = lce.p_creation_date
group by 1,2
order by 1,2