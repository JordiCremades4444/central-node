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
        and (fe.experiment_toggle_id = 'ZAP_CATEGORY_LANDING_PAGE' or fe.experiment_toggle_id = 'ZAP_CATEGORY_LANDING_PAGE_FOR_RETAIL')
)

-- =====================================
-- Funnel
-- =====================================

    ,map_category_opened as ( -- cateogy end possibilities are Food, Health, Groceries, Shops, Smoking and Specialties
        select distinct
            -- co.p_creation_date,
            -- co.country,
            co.category_id,
            co.category_tag as category
        from delta.customer_behaviour_odp.enriched_custom_event__category_opened_v3 co
        inner join calendar_dates
            on co.p_creation_date = calendar_dates.calendar_date
        where 1=1
            and co.category_tag in ('Food', 'Health', 'Groceries', 'Shops')
    ),

    map_category_group_opened as (-- category end possibilities are Food, Health, Groceries, Shops
        select distinct
            -- gco.p_creation_date,
            -- gco.country,
            gco.group_id,
            gco.category_tag as category
        from delta.customer_behaviour_odp.enriched_custom_event__category_group_opened_v3 gco
        inner join calendar_dates
            on gco.p_creation_date = calendar_dates.calendar_date
        where 1=1
            and gco.category_tag in ('Food', 'Health', 'Groceries', 'Shops')
    )

,store_wall_events AS (
    select 
        sw.p_creation_date,
        sw.creation_time,
        sw.dynamic_session_id,
        sw.customer_id, --need it for variant group
        coalesce(mco.category, mcgo.category) as category
    from delta.customer_behaviour_odp.enriched_screen_view__stores_v3 sw
    inner join calendar_dates
        on sw.p_creation_date = calendar_dates.calendar_date
    left join map_category_opened mco
        on sw.category_id = mco.category_id
    left join map_category_group_opened mcgo
        on sw.category_group_id = mcgo.group_id 
    where true
)

    ,stores as (
        select distinct
            s.store_id,
            case 
                when store_subvertical3 in ('Food - Food', 'Food - Other') then 'Food' 
                when store_subvertical3 in ('Smoking') then 'Shops'
                else store_subvertical3 end as category
        from delta.partner_stores_odp.stores_v2 s
        where true
            and s.p_end_date is null
    )

,orders_created as (
    select 
        oc.p_creation_date,
        oc.creation_time,
        oc.dynamic_session_id,
        s.category
    from delta.customer_behaviour_odp.enriched_custom_event__order_created_v3 oc
    inner join calendar_dates
        on oc.p_creation_date = calendar_dates.calendar_date
    inner join stores s
        on oc.store_id = s.store_id
    where true
)

select
    sw.p_creation_date,
    sw.category,
    ce.variant,
    count(distinct sw.dynamic_session_id) as n_sessions_sw,
    count(distinct oc.dynamic_session_id) as n_sessions_orders
from store_wall_events sw
left join customer_exposure ce
    on sw.customer_id = ce.customer_id
    and sw.p_creation_date between ce.start_date and ce.end_date
left join orders_created oc
    on oc.p_creation_date = sw.p_creation_date
    and oc.dynamic_session_id = sw.dynamic_session_id
    and oc.creation_time >= sw.creation_time
    and oc.category = sw.category
where true
group by 1,2,3
order by 1,2