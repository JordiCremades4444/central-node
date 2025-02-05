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

-- Collection Opened
,collection_opened as (
    select 
        event_id,
        dynamic_session_id,
        custom_attributes__store_address_id as store_address_id,
        s.vertical
    from sensitive_delta.customer_mpcustomer_odp.custom_event cu
    inner join calendar_dates cd
        on cd.calendar_date = cu.creation_date
    left join stores s
        on s.store_address_id = cast(cu.custom_attributes__store_address_id as bigint)
    where true
        and cu.event_name = 'Collection Opened'
        and custom_attributes__collection_type = 'Catalogue'
)

-- Bottom Reached
,bottom_reached as (
    select 
        event_id,
        dynamic_session_id,
        custom_attributes__store_address_id as store_address_id
    from sensitive_delta.customer_mpcustomer_odp.custom_event cu
    inner join calendar_dates cd
        on cd.calendar_date = cu.creation_date
    where true
        and cu.event_name = 'Screen Bottom Reached'
        and custom_attributes__collection_type = 'Catalogue'
)

select 
    co.vertical,
    count(distinct co.event_id) as n_collection_opened,
    count(distinct br.event_id) as n_bottom_reached,
    1.000*count(distinct br.event_id)/count(distinct co.event_id) as perc_bottom_reached
from collection_opened co
left join bottom_reached br
    on co.dynamic_session_id = br.dynamic_session_id
    and co.store_address_id = br.store_address_id
group by 1