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
        and custom_attributes__store_address_id = XXX
)

-- =====================================
-- Product category mapping
-- =====================================

,map_category_opened as ( -- cateogy end possibilities are Food, Health, Groceries, Shops, Smoking and Specialties
    select
        p_creation_date,
        country,
        category_id,
        CASE WHEN category_sub_tag IN ('Smoking', 'Specialties') THEN category_sub_tag ELSE category_tag END AS category
    from delta.customer_behaviour_odp.enriched_custom_event__category_opened_v3
    where 1=1
        and category_tag in ('Food', 'Health', 'Groceries', 'Shops')
        and sa.p_creation_date in (select calendar_date from calendar_dates)
),

map_category_group_opened as (-- category end possibilities are Food, Health, Groceries, Shops
    select distinct
        p_creation_date,
        country,
        group_id,
        category_tag AS category,
    from delta.customer_behaviour_odp.enriched_custom_event__category_group_opened_v3
    where 1=1
        and category_tag in ('Food', 'Health', 'Groceries', 'Shops')
        and sa.p_creation_date in (select calendar_date from calendar_dates)
),

-- =====================================
-- Product funnel
-- =====================================

homes as (
    select
        h.p_creation_date,
        h.creation_time,
        h.dynamic_session_id,
        h.event_id
    from delta.customer_behaviour_odp.enriched_screen_view__home_v3 h
    where 1=1
        and gco.p_creation_date in (select calendar_date from calendar_dates)
)

group_category_opened as (
    select
        gco.p_creation_date,
        gco.creation_time,
        gco.dynamic_session_id,
        gco.event_id
    from delta.customer_behaviour_odp.enriched_custom_event__category_group_opened_v3 gco
    where 1=1
        and gco.p_creation_date in (select calendar_date from calendar_dates)
)

,category_opened as (
    select
        co.p_creation_date,
        co.creation_time,
        co.dynamic_session_id,
        co.event_id
    from delta.customer_behaviour_odp.enriched_custom_event__category_opened_v3 co
    where 1=1
        and co.p_creation_date in (select calendar_date from calendar_dates)
)

,store_wall_events AS (
    select 
        sw.p_creation_date,
        sw.creation_time,
        sw.dynamic_session_id,
        sw.event_id
    from delta.customer_behaviour_odp.enriched_screen_view__stores_v3 sw
    where 1=1
        and co.p_creation_date in (select calendar_date from calendar_dates)
)

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

,orders_created as (
    select 
        oc.p_creation_date,
        oc.creation_time,
        oc.dynamic_session_id,
        oc.event_id
    from delta.customer_behaviour_odp.enriched_custom_event__order_created_v3 oc
    where true
        and oc.p_creation_date in (select calendar_date from calendar_dates)
)

select 
    p_creation_date,
from 

-- =====================================
-- Product user classification
-- =====================================

with calendar_dates as (select
    calendar_date
    from unnest(sequence(
        date({start_date}),
        date({end_date}),
        interval '1' day
    )) as dates (calendar_date)
    where true
)

,group_calendar_dates as (
    select
        calendar_date
    from unnest(sequence(
        date_add('day', -{days_between_start_date_and_first_exposure}, date({start_date})),
        date({end_date}),
        interval '1' day
    )) as dates (calendar_date)
    where true
)

,glovo_customers as (
    select
        u.user_id as customer_id
    from delta.central_users_odp.users_v2 u
    where true
        and not user_is_staff
        and not user_is_glovo_employee
        and user_type = 'Customer'
)

,customer_exposure as (
    select distinct
        fe.allocation_key as customer_id
        ,fe.variant
        ,date(fe.first_exposure_datetime) as start_date
        ,coalesce(lag(date(fe.first_exposure_datetime)) over (partition by fe.allocation_key order by date(fe.first_exposure_datetime) desc), date({end_date})) as end_date
    from delta.mlp__experiment_first_exposure__odp.first_exposure fe
    inner join glovo_customers gc
        on fe.allocation_key = gc.customer_id
    where true
        and fe.p_first_exposure_date in (select calendar_date from group_calendar_dates)
        and (fe.experiment_toggle_id = XXX)
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