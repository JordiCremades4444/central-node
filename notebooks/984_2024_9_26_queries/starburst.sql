-- =====================================
-- Calendar dates
-- =====================================

with calendar_dates as (select
    calendar_date
    from unnest(sequence(
        date({XXX}),
        date({XXX}),
        interval '1' day
    )) as dates (calendar_date)
    where true
)

-- =====================================
-- Custom event
-- =====================================

,custom_event as (
    select
        creation_date,
        cu.creation_time,
        cu.dynamic_session_id,
        cu.event_id
    from sensitive_delta.customer_mpcustomer_odp.custom_event
    where true
        and creation_date in (select calendar_date from calendar_dates)
        and event_name = XXX
)

-- =====================================
-- Product funnel
-- =====================================

,store_accessed as (
    select
        sa.p_creation_date,
        sa.creation_time,
        sa.dynamic_session_id,
        sa.event_id
    from delta.customer_behaviour_odp.enriched_custom_event__store_accessed_v3 sa
    where true
        and sa.p_creation_date in (select calendar_date from calendar_dates)
)

-- =====================================
-- Stores
-- =====================================

-- stores of a given sub business unit
,stores as (
    select distinct
        sa.store_address_id,
        sa.store_id
    from delta.partner_stores_odp.store_addresses_v2 sa
    left join delta.partner_stores_odp.stores_v2 s
        on sa.store_id = s.store_id
    left join join delta.partner_stores_odp.store_entity_tags et
        on sa.store_id = et.store_id
    left join join delta.central_geography_odp.cities_v2 c
        on s.city_code = c.city_code
    where true
        and s.p_end_date is null
        and s.p_end_date is null
        and et.pend_date is null
        and s.store_vertical = XXX -- Qcommerce, Food
        and s.store_subvertical2 = XXX -- QCPartners, MFC, Food - Other, Food - Food
        and s.store_subvertical3 = XXX -- Food - Food, Food - Other, Retail, Groceries
        and s.store_subvertical4 = XXX -- Food - Food, Food - Other, Smoking, Health, Retail, Shops
        and contains(et.store_tags, 'Pharmacy OTC') 
)