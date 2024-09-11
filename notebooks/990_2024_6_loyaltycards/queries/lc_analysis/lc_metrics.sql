with calendar_dates as (
    select
        calendar_date
    from unnest(sequence(date({start_date}),date({end_date}),interval '1' day)) as dates (calendar_date)
    where 1=1
)

,group_calendar_dates as (
    select
        calendar_date
    from unnest(sequence(date_add('day', -30, date({start_date})),date({end_date}),interval '1' day)) as dates (calendar_date)
    where 1=1
)

,not_glovo_employees as (
    select
        user_id as customer_id
    from delta.central_users_odp.users_v2
    where true
        and not user_is_staff
        and not user_is_glovo_employee
        and user_type = 'Customer'
)

,lc_groups as (
    select
        allocation_key as customer_id
        ,variant
        ,first_exposure_datetime as start_time
        ,coalesce(lag(first_exposure_datetime) over (partition by allocation_key order by first_exposure_datetime desc), current_date) as end_time
    from delta.mlp__experiment_first_exposure__odp.first_exposure
    inner join not_glovo_employees
        on first_exposure.allocation_key = not_glovo_employees.customer_id
    where 1=1
        and p_first_exposure_date in (select calendar_date from group_calendar_dates)
        and experiment_toggle_id = 'SONIC_LOYALTY_CARDS_NEW'
)

,groceries_stores_ids as (
    select distinct
        s.store_id
    from delta.partner_stores_odp.stores_v2 s
    where 1=1
        and lower(s.store_subvertical) = 'qcpartners'
        and s.store_subvertical2 = 'Groceries'
        and p_end_date is null
)

,loyalty_card_stores as (
    select distinct
        trim(store_name) as store_name
        ,store_id as store_id
    from delta.mfc__temp_order_loyalty_cards__odp.temp_order_loyalty_cards_enriched  as order_loyalty_cards
    where 1=1
        and loyalty_card is not null
        and date(p_creation_date) in (select calendar_date from calendar_dates)
        and store_id in (select store_id from groceries_stores_ids)
)

,n_orders_gmv_all_groceries as (
    select
        od.p_creation_date
        ,lcg.variant as variant
        ,count(distinct od.customer_id) as customers_all_groceries
        ,count(distinct od.order_id) as order_all_groceries
        ,coalesce(sum(case when (od.order_final_status = 'DeliveredStatus') then od.order_total_purchase_eur  else null end), 0) as gmv_delivered_all_groceries
    from delta.central_order_descriptors_odp.order_descriptors_v2 od
    inner join lc_groups lcg
        on od.customer_id = lcg.customer_id
        and od.order_created_at >= lcg.start_time 
        and od.order_created_at < lcg.end_time
    where 1=1
        and store_id in (select store_id from groceries_stores_ids) --look at all stores
        --and od.customer_id in (select customer_id from not_glovo_employees) --with the inner join is enough
        and date(p_creation_date) in (select calendar_date from calendar_dates)
    group by 1,2
)

,n_orders_gmv_lc_stores as (
    select
        od.p_creation_date
        ,od.store_id
        ,lcg.variant as variant
        ,count(distinct od.customer_id) as customers_all_orders_from_lcs
        ,count(distinct od.order_id) as orders_all_orders_from_lcs
        ,sum(od.order_total_purchase_eur) as gmv_delivered_all_orders_from_lcs
    from delta.central_order_descriptors_odp.order_descriptors_v2 od
    inner join lc_groups lcg
        on od.customer_id = lcg.customer_id
        and od.order_created_at >= lcg.start_time 
        and od.order_created_at < lcg.end_time
    where 1=1
        and store_id in (select store_id from loyalty_card_stores) --look at stores that showed at leas a lc order
        and date(p_creation_date) in (select calendar_date from calendar_dates)
        --and od.customer_id in (select customer_id from not_glovo_employees) --with the inner join is enough
        and od.order_final_status = 'DeliveredStatus'
    group by 1,2,3
)

,n_orders_loyalty as (
    select
        od.p_creation_date
        ,od.store_id
        ,lcg.variant as variant
        ,count(distinct od.customer_id) as customers_lc_orders_from_lcs
        ,count(distinct od.order_id) as orders_lc_orders_from_lcs
        ,sum(od.order_total_purchase_eur) as gmv_delivered_lc_orders_from_lcs
    from delta.mfc__temp_order_loyalty_cards__odp.temp_order_loyalty_cards_enriched lce
    inner join delta.central_order_descriptors_odp.order_descriptors_v2 od
        on lce.p_creation_date = od.p_creation_date
        and lce.order_id = od.order_id
    inner join lc_groups lcg
        on od.customer_id = lcg.customer_id
        and od.order_created_at >= lcg.start_time 
        and od.order_created_at < lcg.end_time
    where 1=1
        and loyalty_card is not null
        and od.store_id in (select store_id from loyalty_card_stores) --look at stores that showed at leas a lc order
        and date(od.p_creation_date) in (select calendar_date from calendar_dates)
        --and od.customer_id in (select customer_id from not_glovo_employees) --with the inner join is enough
        and od.order_final_status = 'DeliveredStatus'
    group by 1,2,3
)

,cvr as (
    select
        sa.p_creation_date
        ,sa.store_id
        ,lcg.variant as variant
        ,count(distinct sa.dynamic_session_id) as sa_events_from_lcs
        ,count(distinct case when oc.dynamic_session_id is not null then oc.dynamic_session_id else null end) as cvr_events_from_lcs
    from delta.customer_behaviour_odp.enriched_custom_event__store_accessed_v3 sa
    left join delta.customer_behaviour_odp.enriched_custom_event__order_created_v3 oc
        on sa.dynamic_session_id = oc.dynamic_session_id
        and sa.creation_time <= oc.creation_time
        and sa.store_id = oc.store_id
        and sa.p_creation_date = oc.p_creation_date
    inner join lc_groups lcg
        on sa.customer_id = lcg.customer_id
        and sa.creation_time >= lcg.start_time 
        and sa.creation_time < lcg.end_time
    where 1=1
        and sa.store_id in (select store_id from loyalty_card_stores) --look at stores that showed at leas a lc order
        and date(sa.p_creation_date) in (select calendar_date from calendar_dates)
        --and od.customer_id in (select customer_id from not_glovo_employees) --with the inner join is enough
    group by 1,2,3
)


select 
    --cvr metrics
    cvr.p_creation_date
    ,cvr.store_id
    ,cvr.variant
    ,cvr.sa_events_from_lcs
    ,cvr.cvr_events_from_lcs
    --orders lc in lc stores
    ,n_orders_loyalty.customers_lc_orders_from_lcs
    ,n_orders_loyalty.orders_lc_orders_from_lcs
    ,n_orders_loyalty.gmv_delivered_lc_orders_from_lcs
    --all orders in lc stores
    ,n_orders_gmv_lc_stores.customers_all_orders_from_lcs
    ,n_orders_gmv_lc_stores.orders_all_orders_from_lcs
    ,n_orders_gmv_lc_stores.gmv_delivered_all_orders_from_lcs
    --all orders in all groceries
    ,n_orders_gmv_all_groceries.customers_all_groceries
    ,n_orders_gmv_all_groceries.order_all_groceries
    ,n_orders_gmv_all_groceries.gmv_delivered_all_groceries
from cvr
left join n_orders_loyalty
    on cvr.p_creation_date = n_orders_loyalty.p_creation_date
    and cvr.store_id = n_orders_loyalty.store_id
    and cvr.variant = n_orders_loyalty.variant
left join n_orders_gmv_lc_stores
    on cvr.p_creation_date = n_orders_gmv_lc_stores.p_creation_date
    and cvr.store_id = n_orders_gmv_lc_stores.store_id
    and cvr.variant = n_orders_gmv_lc_stores.variant
left join n_orders_gmv_all_groceries
    on cvr.p_creation_date = n_orders_gmv_all_groceries.p_creation_date
    and cvr.variant = n_orders_gmv_all_groceries.variant