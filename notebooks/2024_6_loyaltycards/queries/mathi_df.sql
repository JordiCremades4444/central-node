with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date('{start_date}'),-- initial date
        date('{end_date}'),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,groceries_stores as (
    select 
        sa.store_address_id,
        sa.store_id
    from delta.partner_stores_odp.store_addresses_v2 sa
    left join delta.partner_stores_odp.stores_v2 s 
        on sa.store_id = s.store_id
    where 1=1
        and s.store_subvertical2 = 'Groceries'
        and s.p_end_date is null
        and sa.p_end_date is null
)

,customer_exposures_dates as (
    select
        customer_id,
        min(p_creation_date) as exposure_date
    from delta.customer_behaviour_odp.dynamic_sessions_v1 AS ds
    inner join unnest(ds.visited_store_address_ids) as u(unnested_store_address_id) on true
    inner join groceries_stores gs on u.unnested_store_address_id = gs.store_address_id
    where 1=1 
        and p_creation_date in (select calendar_date from calendar_dates)
        and customer_id is not null
        and count_ce__store_accessed > 0 
    group by 1
)


,sessions_with_groceries_store_accessed as (
    select
        ds.dynamic_session_id,
        ds.p_creation_date,
        ds.customer_id
    from delta.customer_behaviour_odp.dynamic_sessions_v1 AS ds
    inner join unnest(ds.visited_store_address_ids) as u(unnested_store_address_id) on true
    inner join groceries_stores gs on u.unnested_store_address_id = gs.store_address_id
    where 1=1
        and p_creation_date in (select calendar_date from calendar_dates)
        and customer_id is not null
        and count_ce__store_accessed > 0 
)

,sessions_with_groceries_order_created as (
    select
        oc.dynamic_session_id,
        oc.customer_id,
        count(distinct oc.order_id) as count_orders_created_in_session
    from delta.customer_behaviour_odp.enriched_custom_event__order_created_v3 as oc
    inner join groceries_stores on groceries_stores.store_id = oc.store_id
    where 1=1 
        and p_creation_date in (select calendar_date from calendar_dates)
        and customer_id is not null
    group by 1,2
)

,groceries_orders as (
    select 
        o.customer_id,
        o.order_id,
        o.order_total_purchase_eur,
        o.p_creation_date
    from delta.central_order_descriptors_odp.order_descriptors_v2 as o
    inner join groceries_stores s on s.store_id = o.store_id 
    where 1=1 
        and p_creation_date in (select calendar_date from calendar_dates)
)

,observations_from_sessions_stage as (
    select
        sa.customer_id,
        sa.p_creation_date as p_event_date,
        count(distinct sa.dynamic_session_id) as sessions_store_accessed,
        count(distinct oc.dynamic_session_id) as sessions_with_orders_created,
        count(distinct go.order_id) as groceries_orders,
        sum(coalesce(go.order_total_purchase_eur,0)) as sum_purchase_eur
    from sessions_with_groceries_store_accessed as sa
    left join sessions_with_groceries_order_created as oc 
        on sa.dynamic_session_id=oc.dynamic_session_id and sa.customer_id=oc.customer_id
    left join groceries_orders as go 
        on sa.customer_id=go.customer_id AND sa.p_creation_date=go.p_creation_date
    where 1=1
        and sa.p_creation_date in (select calendar_date from calendar_dates)
    group by 1,2
)

,observations_from_sessions as (
    select 
        t1.customer_id, 
        t1.p_event_date, 
        t2.obs_name, 
        t2.obs_value
    from observations_from_sessions_stage t1
    cross join unnest (
        array['sessions_store_accessed', 'sessions_with_orders_created', 'sum_purchase_eur', 'groceries_orders'],
        array[sessions_store_accessed, sessions_with_orders_created, sum_purchase_eur, groceries_orders]
    ) t2 (obs_name, obs_value)
)

,observations_daily as (
    select
        e.customer_id,
        e.exposure_date,
        ods.p_event_date,
        ods.obs_name as observation_name,
        ods.obs_value as observation_value
    from customer_exposures_dates e
    left join observations_from_sessions ods
        on e.customer_id = ods.customer_id
    where 1=1
        and (p_event_date >= exposure_date)
)

,observations_daily_with_exposure_date as (
    select
        od.customer_id,
        od.exposure_date,
        od.p_event_date,
        od.observation_name,
        od.observation_value,
        --how many days passed since the first exposure
        cast(date_diff('day', exposure_date, p_event_date) as INT) as experiment_days,
        --how many weeks passed since the first day
        cast(floor(date_diff('day', (select min(calendar_date) from calendar_dates), p_event_date))/7 as int) + 1 as obs_week_index_since_start_date,
        --how many weeks passed since the first exposure
        cast(floor(date_diff('day', (select min(calendar_date) from calendar_dates), exposure_date))/7 as int) + 1 as exposure_week_index_since_start_date
    from observations_daily od
)

select
    * 
from observations_daily_with_exposure_date 
where 1=1
    customer_id=92047749

-- observations_daily_enriched as (
-- select
-- customer_id,
-- observation_name,
-- exposure_date,
-- exposure_week_index_since_start_date,
-- obs_week_index_since_start_date,
-- sum(sum(observation_value)) over (
--     partition by customer_id, exposure_date, exposure_week_index_since_start_date, observation_name
--     order by obs_week_index_since_start_date) as cumsum_observation_value
-- from observations_daily_with_exposure_date
-- group by 1,2,3,4,5
-- ),

-- observations_daily_final as (
-- select * from observations_daily_enriched
-- where customer_id is not null
-- )


-- ,
-- -- select from observations_daily_final pivoting in a way to have customer_id, exposure_date, exposure_week_index_since_start_date, obs_week_index_since_start_date as unique indexes, 
-- -- distinct values of cumsum_observation_value as the columns, and cumsum_observation_value as the values. Fill the missing values with zeros.
-- observations_pivoted AS (
--     SELECT
--         f.customer_id,
--         f.exposure_date,
--         f.exposure_week_index_since_start_date,
--         f.obs_week_index_since_start_date,
--         SUM(CASE WHEN observation_name = 'sessions_store_accessed' THEN cumsum_observation_value ELSE 0 END) AS sessions_store_accessed,
--         SUM(CASE WHEN observation_name = 'sessions_with_orders_created' THEN cumsum_observation_value ELSE 0 END) AS sessions_with_orders_created,
--         SUM(CASE WHEN observation_name = 'groceries_orders' THEN cumsum_observation_value ELSE 0 END) AS groceries_orders,
--         SUM(CASE WHEN observation_name = 'sum_purchase_eur' THEN cumsum_observation_value ELSE 0 END) AS sum_purchase_eur
    
--     FROM observations_daily_final AS f
--     INNER JOIN (
--                 SELECT
--                     user_id as customer_id
--                 FROM "delta"."central_users_odp"."users_v2"
--                 WHERE True
--                     and not user_is_staff
--                     and not user_is_glovo_employee
--                     and user_type = 'Customer'
--             ) as u
--                 on f.customer_id = u.customer_id
--     GROUP BY 1,2,3,4
-- )

-- SELECT *
-- FROM observations_pivoted