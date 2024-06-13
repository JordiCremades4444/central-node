with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date({start_date}),-- initial date
        date({end_date}),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,top_cities as (
    select
        od.order_city_code,
        od.order_country_code,
        count(distinct od.order_id) as n_orders
    from delta.central_order_descriptors_odp.order_descriptors_v2 od
    inner join delta.partner_stores_odp.stores_v2 s
        on od.store_id = s.store_id
        and s.p_end_date is null
    where 1=1
        and od.p_creation_date in (select calendar_date from calendar_dates)
        and s.store_subvertical2 = 'Groceries'
    group by 1,2
    order by 3 desc
    limit {top}
)

,do_not_consider_customer_ids as (
    select distinct 
        data__experimentation_allocation_key as customer_id
    from delta.mlp_feature_store_experiment_exposure_odp.mlp_experiment_exposure
    where 1=1
        and p_event_date in (select calendar_date from calendar_dates)
        and data__experimentation_toggle_id in ('ZAP_CATEGORY_LANDING_PAGE', 'ZAP_NSW_EXPERIMENT')
        and data__experimentation_variant_value in (
            'forced_assignment' -- nsw
            ,'control_outofrange','forced_assignment_1','control_ft','control_ft' --instore
        )
)

,nsw_groups as (
    select
        experiment_toggle_id,
        allocation_key as customer_id,
        variant,
        first_exposure_datetime as start_time,
        coalesce(lag(first_exposure_datetime) over (partition by allocation_key order by first_exposure_datetime desc), current_date) as end_time
    from delta.mlp__experiment_first_exposure__odp.first_exposure
    where 1=1
        and p_first_exposure_date in (select calendar_date from calendar_dates)
        and experiment_toggle_id = 'ZAP_CATEGORY_LANDING_PAGE'
        and allocation_key not in (select customer_id from do_not_consider_customer_ids)
)

,instore_prices_groups as (
    select
        experiment_toggle_id,
        allocation_key as customer_id,
        variant,
        first_exposure_datetime as start_time,
        coalesce(lag(first_exposure_datetime) over (partition by allocation_key order by first_exposure_datetime desc), current_date) as end_time
    from delta.mlp__experiment_first_exposure__odp.first_exposure
    where 1=1
        and p_first_exposure_date in (select calendar_date from calendar_dates)
        and experiment_toggle_id = 'ZAP_NSW_EXPERIMENT'
        and allocation_key not in (select customer_id from do_not_consider_customer_ids)
)

,order_events_metrics as (
    select 
        oc.country,
        oc.city,
        oc.p_creation_date,
        oc.customer_id,
        count(distinct oc.order_id) as groceries_orders_count,
        count(distinct case when roi.order_subvertical2_is_ret1 then oc.order_id else null end) as groceries_ret_orders_count,
        count(distinct case when roi.store_name_is_ret1 then oc.order_id else null end) as stores_ret_orders_count,
        sum(od.order_total_purchase_eur) as groceries_gmv_sum
    from delta.customer_behaviour_odp.enriched_backend_event__checkout_order_created_v3 oc
    inner join delta.partner_stores_odp.stores_v2
        on stores_v2.p_end_date is null
        and stores_v2.store_subvertical2 = 'Groceries'
        and stores_v2.store_id = oc.store_id
    left join delta.central_order_descriptors_odp.order_descriptors_v2 od
        on oc.order_id = od.order_id
        and oc.p_creation_date = od.p_creation_date
    left join delta.central__retention_orders__odp.retention_order_info roi
        on oc.order_id = roi.order_id
        and oc.p_creation_date = od.p_creation_date
    where 1=1
        and oc.p_creation_date in (select calendar_date from calendar_dates)
        and oc.customer_id not in(select customer_id from do_not_consider_customer_ids)
        and oc.city in (select order_city_code from top_cities)
    group by 1,2,3,4
)

-------------------------
--- Groceries session ---
-------------------------

,groceries_stores as (
    select 
        store_id, 
        store_subvertical2
    from delta.partner_stores_odp.stores_v2
    where 1=1
        and store_subvertical2 = 'Groceries'
        and p_end_date is null
)

,qcom_category_id_list as (
    select distinct
        category_id, 
        category_tag
    from delta.customer_behaviour_odp.enriched_custom_event__category_opened_v3
    where 1=1
        and p_creation_date in (select calendar_date from calendar_dates)
        and category_tag in ('Groceries', 'Shops', 'Health')
)

,all_sessions as (
    select distinct
        p_creation_date, 
        customer_id, 
        dynamic_session_id,
        city_code,
        country_code
    from delta.customer_behaviour_odp.dynamic_sessions_v1
    where 1=1
        and p_creation_date in (select calendar_date from calendar_dates)
)

--- 1. Bubbles
,category_group_events as (
    select 
        p_creation_date, 
        event_id, 
        dynamic_session_id, 
        customer_id, 
        category_tag
    from delta.customer_behaviour_odp.enriched_custom_event__category_group_opened_v3
    where 1=1
        and p_creation_date in (select calendar_date from calendar_dates)
        and category_tag in ('Groceries', 'Shops', 'Health')
)

--- 1.1 Sub-Bubbles
,category_events as (
    select 
        p_creation_date, 
        event_id, 
        dynamic_session_id, 
        category_id, 
        category_name, 
        category_tag, 
        category_sub_tag, 
        customer_id
    from delta.customer_behaviour_odp.enriched_custom_event__category_opened_v3
    where 1=1
        and p_creation_date in (select calendar_date from calendar_dates)
        and category_tag in ('Groceries', 'Shops', 'Health')
)

--- 2. Store wall
,store_walls_events as (
    select 
        sw.p_creation_date, 
        sw.event_id, 
        sw.dynamic_session_id, 
        sw.country, 
        sw.creation_time, 
        sw.customer_id, 
        cl.category_id, 
        cl.category_tag, 
        sw.platform
    from delta.customer_behaviour_odp.enriched_screen_view__stores_v3 as sw
    inner join qcom_category_id_list AS cl 
        on sw.category_id = cl.category_id
    where 1=1
        and p_creation_date in (select calendar_date from calendar_dates)
)

--- 3. Store details
,store_events as (
    select 
        sd.p_creation_date, 
        sd.event_id, 
        sd.dynamic_session_id, 
        sd.customer_id, 
        gs.store_id, 
        gs.store_subvertical2, 
        sd.platform
    from delta.customer_behaviour_odp.enriched_screen_view__store_v3 as sd
    inner join groceries_stores as gs 
        on sd.store_id = gs.store_id
    where 1=1 
        and sd.p_creation_date in (select calendar_date from calendar_dates)
)

--- 4. Checkout
,checkout_events as (
    select 
        chk.p_creation_date, 
        chk.event_id, 
        chk.dynamic_session_id, 
        chk.creation_time_local, 
        chk.creation_time, 
        chk.customer_id, 
        gs.store_id, 
        gs.store_subvertical2, 
        chk.platform
    from delta.customer_behaviour_odp.enriched_screen_view__checkout_template_received_v3 as chk
    inner join groceries_stores as gs 
        on chk.store_id = gs.store_id
    where 1=1 
        and chk.p_creation_date in (select calendar_date from calendar_dates)
)

--- 5. Order created
,order_events as (
    select 
        oc.p_creation_date, 
        oc.event_id, 
        oc.dynamic_session_id, 
        oc.creation_time_local, 
        oc.creation_time, 
        oc.customer_id, 
        gs.store_id, 
        gs.store_subvertical2, 
        oc.platform
    from delta.customer_behaviour_odp.enriched_backend_event__checkout_order_created_v3 as oc
    inner join groceries_stores as gs 
        on oc.store_id = gs.store_id
    where 1=1 
        and oc.p_creation_date in (select calendar_date from calendar_dates)
)

,sessions_metrics as (
    select 
        ase.country_code,
        ase.city_code,
        ase.p_creation_date,
        ase.customer_id,
        count(distinct ase.dynamic_session_id) as all_session_count,
        count(distinct coalesce(cge.dynamic_session_id, ce.dynamic_session_id, swe.dynamic_session_id, se.dynamic_session_id, chk.dynamic_session_id, oe.dynamic_session_id)) as groceries_session_count
    from all_sessions as ase
    left join category_group_events as cge 
        on ase.dynamic_session_id = cge.dynamic_session_id 
        and cge.category_tag = 'Groceries'
    left join category_events as ce 
        on ase.dynamic_session_id = ce.dynamic_session_id 
        and ce.category_tag = 'Groceries'
    left join store_walls_events as swe 
        on ase.dynamic_session_id = swe.dynamic_session_id 
        and swe.category_tag = 'Groceries'
    left join store_events as se 
        on ase.dynamic_session_id = se.dynamic_session_id 
        and se.store_subvertical2 = 'Groceries'
    left join checkout_events as chk 
        on ase.dynamic_session_id = chk.dynamic_session_id 
        and chk.store_subvertical2 = 'Groceries'
    left join order_events as oe 
        on ase.dynamic_session_id = oe.dynamic_session_id 
        and oe.store_subvertical2 = 'Groceries'
    where 1=1
        and ase.city_code in (select order_city_code from top_cities)
        and ase.customer_id not in(select customer_id from do_not_consider_customer_ids)
    group by 1,2,3,4
)

select 
    sm.p_creation_date,
    sm.country_code,
    sm.city_code,
    nswg.variant as nswg_variant,
    ipg.variant as ipg_variant,
    sum(sm.all_session_count) as all_session_count,
    sum(sm.groceries_session_count) as groceries_session_count,
    sum(ovm.groceries_orders_count) as groceries_orders_count,
    sum(ovm.groceries_ret_orders_count) as groceries_ret_orders_count,
    sum(ovm.stores_ret_orders_count) as stores_ret_orders_count,
    sum(ovm.groceries_gmv_sum) as groceries_gmv_sum
from sessions_metrics sm
left join order_events_metrics ovm
    on sm.p_creation_date = ovm.p_creation_date
    and sm.customer_id = ovm.customer_id
left join nsw_groups nswg
    on nswg.customer_id = sm.customer_id
    and date(nswg.start_time) <= sm.p_creation_date
    and date(nswg.end_time) > sm.p_creation_date
left join instore_prices_groups ipg
    on ipg.customer_id = sm.customer_id
    and date(ipg.start_time) <= sm.p_creation_date
    and date(ipg.end_time) > sm.p_creation_date
where 1=1 
group by 1,2,3,4,5
order by 2,3,4,5,1


