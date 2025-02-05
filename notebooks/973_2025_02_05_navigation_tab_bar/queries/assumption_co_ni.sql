with calendar_dates as (select
    calendar_date
    from unnest(sequence(date({start_date}),date({end_date}),interval '1' day)) as dates (calendar_date)
    where true
)

,stores as (
    select distinct
        sa.store_address_id,
        sa.store_id,
        case 
            when store_subvertical2 in ('Food - Food', 'Food - Other') then 'Food'
            when store_subvertical2 in ('Groceries') then 'Groceries'
            when store_subvertical2 in ('Retail') then 'Retail'
            else null
        end as vertical
    from delta.partner_stores_odp.store_addresses_v2 sa
    left join delta.partner_stores_odp.stores_v2 s
        on sa.store_id = s.store_id
    where true
        and sa.p_end_date is null
        and s.p_end_date is null
)

,co as (
    select 
        date(cu.creation_time) as p_creation_date,
        s.vertical,
        count(distinct event_id) as n_events,
        count(distinct dynamic_session_id) as n_sessions
    from sensitive_delta.customer_mpcustomer_odp.custom_event cu
    inner join calendar_dates cd
        on cd.calendar_date = cu.creation_date
    left join stores s
        on cast(s.store_address_id as varchar) = cu.custom_attributes__store_address_id
    where true
        and cu.event_name = 'Collection Opened'
    group by 1,2
    order by 1,2
)

,ni as (
    select 
        date(cu.creation_time) as p_creation_date,
        s.vertical,
        count(distinct event_id) as n_events,
        count(distinct dynamic_session_id) as n_sessions
    from sensitive_delta.customer_mpcustomer_odp.custom_event cu
    inner join calendar_dates cd
        on cd.calendar_date = cu.creation_date
    left join stores s
        on cast(s.store_address_id as varchar) = cu.custom_attributes__store_address_id
    where true
        and cu.event_name = 'Store Mobile Navigation Bar Impression'
    group by 1,2
    order by 1,2
)

select 
    co.p_creation_date,
    co.vertical,
    co.n_events as co_n_events,
    co.n_sessions as co_n_sessions,
    ni.n_events as ni_n_events,
    ni.n_sessions as ni_n_sessions
from co
left join ni
    on ni.p_creation_date = co.p_creation_date
    and ni.vertical = co.vertical