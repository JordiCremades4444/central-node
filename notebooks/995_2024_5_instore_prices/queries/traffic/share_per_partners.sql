with calendar_dates as (select
        calendar_date
    from unnest(sequence(date({start_date}),date({end_date}),interval '1' day)) as dates (calendar_date)
    where 1=1
)

, top_cities as (
    select 
        od.order_city_code as city
        ,count(distinct s.store_id) n_city_store_ids
        ,count(distinct od.order_id) n_city_orders
    from delta.central_order_descriptors_odp.order_descriptors_v2 od
    inner join delta.partner_stores_odp.stores_v2 s
        on od.store_id = s.store_id
        and s.p_end_date is null
        and s.store_subvertical = 'QCPartners'
        and s.store_subvertical2 = 'Groceries'
    where 1=1
        and od.p_creation_date in (select calendar_date from calendar_dates)
    group by 1
    order by 3 desc
    limit 150
)

,partners_info as (
    select
        s.city_code as city
        ,s.store_id
        ,s.store_name
        ,min(date(s.start_date)) first_date_instore_price_enabled
    from delta.partner_stores_odp.stores_v2 s
    inner join top_cities tc
        on tc.city = s.city_code
    where 1=1
        and s.store_subvertical = 'QCPartners'
        and s.store_subvertical2 = 'Groceries'
        and s.store_is_in_store_prices_enabled
    group by 1,2,3
)

,city_info as (
    select 
        sa.city
        ,sa.p_creation_date
        ,count(distinct sa.dynamic_session_id) n_sessions_city_info
    from delta.customer_behaviour_odp.enriched_custom_event__store_accessed_v3 sa
    inner join calendar_dates cd
        on cd.calendar_date = sa.p_creation_date
    inner join top_cities tc
        on tc.city = sa.city
    where 1=1
        and sa.store_subvertical2 = 'Groceries'
        and sa.store_subvertical = 'QCPartners'
    group by 1,2
)

,output as (
    select
        cd.calendar_date
        ,sa.store_id
        -- partner info
        ,pi.city
        ,pi.store_name
        ,pi.first_date_instore_price_enabled
        -- city info
        ,ci.n_sessions_city_info
        -- count
        ,count(distinct sa.dynamic_session_id) as n_sessions_partner
    from calendar_dates cd
    left join delta.customer_behaviour_odp.enriched_custom_event__store_accessed_v3 sa
        on cd.calendar_date = sa.p_creation_date
        and sa.city in (select city from top_cities)
        and sa.store_id in (select store_id from partners_info)
    left join partners_info pi
        on sa.store_id = pi.store_id
    left join city_info ci
        on pi.city = ci.city
        and cd.calendar_date = ci.p_creation_date
    group by 1,2,3,4,5,6
)

select * from output


