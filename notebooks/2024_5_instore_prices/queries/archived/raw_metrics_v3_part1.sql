with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date({start_date}),-- initial date
        date({end_date}),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,calendar_dates_groups as (select
        calendar_date
    from unnest(sequence(
        date({start_date_groups}),-- initial date
        date({end_date_groups}),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,do_not_consider_staff as (
    select distinct
        user_id as customer_id
    from delta.central_users_odp.users_v2
    where 1=1
        and user_is_staff
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
        and od.p_creation_date between date('2024-02-01') and date('2024-05-31')
        and s.store_subvertical2 = 'Groceries'
    group by 1,2
    order by 3 desc
    limit {top}
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
        and p_first_exposure_date in (select calendar_date from calendar_dates_groups)
        and experiment_toggle_id = 'ZAP_NSW_EXPERIMENT'
        and allocation_key not in (select customer_id from do_not_consider_staff)
)

,order_events_metrics as (
    select 
        oc.country,
        oc.city,
        oc.p_creation_date,
        ipg.variant as ipg_variant,
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
    left join instore_prices_groups ipg
        on ipg.customer_id = oc.customer_id
        and ipg.start_time <= oc.creation_time
        and ipg.end_time > oc.creation_time
    where 1=1
        and oc.p_creation_date in (select calendar_date from calendar_dates)
        and oc.city in (select order_city_code from top_cities)
        and oc.customer_id not in (select customer_id from do_not_consider_staff)
    group by 1,2,3,4
)

select 
    *
from order_events_metrics

