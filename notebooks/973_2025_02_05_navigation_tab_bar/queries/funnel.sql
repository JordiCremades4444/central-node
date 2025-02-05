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

-- Navigation Tab Bar Impression (Denominator)
,navigation_impression as (
    select 
        event_id,
        dynamic_session_id,
        creation_time,
        creation_date,
        s.vertical,
        custom_attributes__store_address_id as store_address_id
    from sensitive_delta.customer_mpcustomer_odp.custom_event cu
    inner join calendar_dates cd
        on cd.calendar_date = cu.creation_date
    left join stores s
        on cast(s.store_address_id as varchar) = cu.custom_attributes__store_address_id
    where true
        and cu.event_name = 'Store Mobile Navigation Bar Impression'
)

-- Navigation Tab Bar Tapped (Numerator)
,navigation_tapped as (
    select 
        event_id,
        dynamic_session_id,
        creation_time,
        creation_date,
        coalesce(custom_attributes__collection_id, custom_attributes__collection_group_id) as element_tapped
    from sensitive_delta.customer_mpcustomer_odp.custom_event cu
    inner join calendar_dates cd
        on cd.calendar_date = cu.creation_date
    where true
        and cu.event_name = 'Navigation Bar Element Tapped'
        and (custom_attributes__collection_type = 'Catalogue' or custom_attributes__collection_type is null) -- Because SuperCollections do not populate this field
)

-- Collection Opened to map Store
,collection_opened as (
    select 
        event_id,
        dynamic_session_id,
        creation_time,
        creation_date,
        custom_attributes__collection_group_id,
        custom_attributes__collection_id,
        custom_attributes__store_address_id as store_address_id
    from sensitive_delta.customer_mpcustomer_odp.custom_event cu
    inner join calendar_dates cd
        on cd.calendar_date = cu.creation_date
    where true
        and cu.event_name = 'Collection Opened'
)

,navigation_tapped_enriched as (
    select distinct
        nt.event_id,
        nt.dynamic_session_id,
        nt.creation_time,
        nt.creation_date,
        co.store_address_id,
        case when nt.element_tapped = co.custom_attributes__collection_id then 'Collection'
             when nt.element_tapped = co.custom_attributes__collection_group_id then 'SuperCollection'
             else 'Other'
        end as element_tapped
    from navigation_tapped nt
    inner join collection_opened co -- We only keep those taps that we can map
        on nt.dynamic_session_id = co.dynamic_session_id
        and co.creation_time between nt.creation_time and nt.creation_time + interval '2' minute
        and (nt.element_tapped = co.custom_attributes__collection_id or nt.element_tapped = co.custom_attributes__collection_group_id) -- To be able to join the two kinds of taps - in collections and supercollections
    where true
)

,funnel as (
    select
        ni.creation_date,
        ni.vertical,
        --sessions
        count(distinct concat(ni.dynamic_session_id,ni.store_address_id)) as bar_impressions_sessions_store,
        count(distinct case when element_tapped = 'Collection' then concat(nt.dynamic_session_id,nt.store_address_id) end) as bar_collection_taps_sessions_store,
        count(distinct case when element_tapped = 'SuperCollection' then concat(nt.dynamic_session_id,nt.store_address_id) end) as bar_supercollection_taps_sessions_store,
        count(distinct concat(nt.dynamic_session_id,nt.store_address_id)) as bar_total_taps_sessions_store
        --events
        -- count(distinct ni.event_id) as bar_impressions_events,
        -- count(distinct case when element_tapped = 'Collection' then nt.event_id end) as bar_collection_taps_events,
        -- count(distinct case when element_tapped = 'SuperCollection' then nt.event_id end) as bar_supercollection_taps_events,
        -- count(distinct nt.event_id) as bar_total_taps_events
    from navigation_impression ni
    left join navigation_tapped_enriched nt
        on ni.dynamic_session_id = nt.dynamic_session_id
        and nt.creation_time between ni.creation_time and ni.creation_time + interval '2' minute
        and nt.store_address_id = ni.store_address_id
    group by 1,2
)

select * from funnel