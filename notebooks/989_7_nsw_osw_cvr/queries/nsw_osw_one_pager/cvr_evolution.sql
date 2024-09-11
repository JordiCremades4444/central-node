with calendar_dates as (
    select
        calendar_date
    from unnest(sequence(date({start_date}),date({end_date}),interval '1' day)) as dates (calendar_date)
    where 1=1
)

,nsw_groups as (
    select
        allocation_key as customer_id,
        variant,
        first_exposure_datetime as start_time,
        coalesce(lag(first_exposure_datetime) over (partition by allocation_key order by first_exposure_datetime desc), date_trunc('second', TIMESTAMP '{end_date}' AT TIME ZONE 'UTC')) as end_time
    from delta.mlp__experiment_first_exposure__odp.first_exposure
    where 1=1
        and p_first_exposure_date in (select calendar_date from calendar_dates)
        and experiment_toggle_id = 'ZAP_CATEGORY_LANDING_PAGE' or experiment_toggle_id = 'ZAP_CATEGORY_LANDING_PAGE_FOR_RETAIL'
)

select * from nsw_groups where customer_id = 56758648

-- ,cat_events as (
--     select distinct
--         date(p_creation_date) as p_creation_date,
--         date_format(p_creation_date, '%Y-%m') as p_creation_month,
--         upper(country) as country,
--         coalesce(category_tag,'category_not_found') as category_tag,
--         dynamic_session_id,
--         creation_time,
--         customer_id
--     from delta.customer_behaviour_odp.enriched_custom_event__category_opened_v3
--     where 1=1
--         and category_tag in ('Groceries', 'Shops', 'Health')
--         and p_creation_date in (select calendar_date from calendar_dates)
-- )

-- ,order_events as (
--     select 
--         date(p_creation_date) as p_creation_date,
--         store_subvertical3,
--         dynamic_session_id,
--         o.creation_time,
--         customer_id
--     from delta.customer_behaviour_odp.enriched_backend_event__checkout_order_created_v3 o
--     inner join delta.partner_stores_odp.stores_v2
--         on stores_v2.p_end_date is null
--         and stores_v2.store_subvertical3 in ('Groceries','Health','Shops')
--         and stores_v2.store_id = o.store_id
--     where 1=1
--         and p_creation_date in (select calendar_date from calendar_dates)
-- )

-- ,funnel as (
--     select
--         c.p_creation_date,
--         c.country,
--         c.category_tag,
--         coalesce(nsw.variant,'group_not_found') as variant,
--         --customers in sw
--         count(distinct c.customer_id) as all_sw_customer_id,
--         count(distinct case when is_recurrent_groceries then c.customer_id else null end) as rc_sw_customer_id,
--         count(distinct case when is_recurrent_groceries = false then c.customer_id else null end) as nc_sw_customer_id,
--         count(distinct case when is_recurrent_groceries is null then c.customer_id else null end) as not_found_sw_customer_id,
--         --session in sw
--         count(distinct c.dynamic_session_id) as all_sw_sessions,
--         count(distinct case when is_recurrent_groceries then c.dynamic_session_id else null end) as rc_sw_sessions,
--         count(distinct case when is_recurrent_groceries = false then c.dynamic_session_id else null end) as nc_sw_sessions,
--         count(distinct case when is_recurrent_groceries is null then c.dynamic_session_id else null end) as not_found_sw_sessions,
--         --sessions in orders
--         count(distinct oe.dynamic_session_id) as all_oe_sessions,
--         count(distinct case when is_recurrent_groceries then oe.dynamic_session_id else null end) as rc_oe_sessions,
--         count(distinct case when is_recurrent_groceries = false then oe.dynamic_session_id else null end) as nc_oe_sessions,
--         count(distinct case when is_recurrent_groceries is null then oe.dynamic_session_id else null end) as not_found_oe_sessions
--     from cat_events c
--     left join order_events oe
--         on c.dynamic_session_id = oe.dynamic_session_id
--         and c.p_creation_date = oe.p_creation_date
--         and oe.creation_time between c.creation_time and date_add('minute', 60, c.creation_time)
--         and c.category_tag = oe.store_subvertical3
--     left join nsw_groups nsw
--         on c.customer_id = nsw.customer_id
--         and c.creation_time >= nsw.start_time
--         and c.creation_time < nsw.end_time
--     left join delta.mfc__sessions_nc_rc__odp.sessions_nc_rc snr
--         on c.dynamic_session_id = snr.dynamic_session_id
--     where 1=1
--     group by 1,2,3,4
-- )

-- select 
--     *
-- from funnel
-- where 1=1
--     and p_creation_date is not null
--     and country is not null
--     and category_tag is not null









--     with calendar_dates as (
--     select
--         calendar_date
--     from unnest(sequence(date({start_date}),date({end_date}),interval '1' day)) as dates (calendar_date)
--     where 1=1
-- )

-- ,nsw_groups as (
--     select
--         allocation_key as customer_id,
--         variant,
--         first_exposure_datetime as start_time,
--         coalesce(lag(first_exposure_datetime) over (partition by allocation_key order by first_exposure_datetime desc), current_date) as end_time
--     from delta.mlp__experiment_first_exposure__odp.first_exposure
--     where 1=1
--         and p_first_exposure_date in (select calendar_date from calendar_dates)
--         and experiment_toggle_id = 'ZAP_CATEGORY_LANDING_PAGE' or experiment_toggle_id = 'ZAP_CATEGORY_LANDING_PAGE_FOR_RETAIL'
-- )

-- ,cat_events as (
--     select distinct
--         date(p_creation_date) as p_creation_date
--         ,upper(city) as city
--         ,category_id
--         ,category_tag
--     from delta.customer_behaviour_odp.enriched_custom_event__category_opened_v3
--     where 1=1
--         --and category_tag in ('Groceries', 'Shops', 'Health', 'Food')
--         and p_creation_date in (select calendar_date from calendar_dates)
-- )

-- select 
--     date(s.p_creation_date) as p_creation_date
--     ,s.category_id
--     ,count(distinct c.category_tag) distinct_category_tags_assigned
--     ,count(distinct s.event_id) n_distinct_events
-- from delta.customer_behaviour_odp.enriched_screen_view__stores_v3 s
-- left join cat_events c
--     on s.category_id = c.category_id
--     and s.city = c.city
--     and s.p_creation_date = c.p_creation_date
-- where 1=1
--     and s.p_creation_date in (select calendar_date from calendar_dates)
--     and s.category_id is not null
-- group by 1,2
-- order by 1,2