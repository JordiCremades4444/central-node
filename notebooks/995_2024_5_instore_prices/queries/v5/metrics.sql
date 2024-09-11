with calendar_dates as (select
        calendar_date
    from unnest(sequence(date({start_date}),date({end_date}),interval '1' day)) as dates (calendar_date)
    where 1=1
)

,instore_prices_groups as (
    select
        experiment_toggle_id
        ,allocation_key as customer_id
        ,variant
        ,first_exposure_datetime as start_time
        ,coalesce(lag(first_exposure_datetime) over (partition by allocation_key order by first_exposure_datetime desc), current_date) as end_time
    from delta.mlp__experiment_first_exposure__odp.first_exposure
    where 1=1
        and p_first_exposure_date in (select calendar_date from calendar_dates)
        and experiment_toggle_id = 'ZAP_NSW_EXPERIMENT'
)

, top_cities as (
    select
        od.order_city_code
        ,od.order_country_code
        ,count(distinct od.order_id) as n_orders
        ,count(distinct s.store_id) as n_stores
        ,count(distinct case when store_is_in_store_prices_enabled then s.store_id else null end) as n_stores_instore_price_enabled
    from delta.central_order_descriptors_odp.order_descriptors_v2 od
    inner join delta.partner_stores_odp.stores_v2 s
        on od.store_id = s.store_id
        and s.p_end_date is null
        and s.store_subvertical = 'QCPartners'
        and s.store_subvertical2 = 'Groceries'
    where 1=1
        and od.p_creation_date in (select calendar_date from calendar_dates)
    group by 1,2
    order by 3 desc
    limit 150
)

,category_events_raw as (
    select
        p_creation_date
        ,dynamic_session_id
        ,customer_id
        ,city
        ,country
        ,creation_time
        ,platform
        ,row_number() over (
            partition by dynamic_session_id,order_id
            order by random()  
        ) as rn -- Order by p_creation_date and random() to break ties
    from delta.customer_behaviour_odp.enriched_custom_event__category_opened_v3
    where 1=1
        and p_creation_date in (select calendar_date from calendar_dates)
        and category_tag = 'Groceries'
        and dynamic_session_id is not null
)

,category_events as (
    select
        *
    from category_events_raw
    where 1=1
        and rn = 1
)

,order_created as (
    select
        oc.dynamic_session_id
        ,oc.p_creation_date
        ,oc.customer_id
        ,oc.city
        ,oc.country
        ,oc.creation_time
        ,oc.platform
        ,oc.order_id as oc_order_id
        ,row_number() over (
            partition by dynamic_session_id,order_id
            order by random()  
        ) as rn -- Order by p_creation_date and random() to break ties
    from delta.customer_behaviour_odp.enriched_backend_event__checkout_order_created_v3 oc
    inner join delta.partner_stores_odp.stores_v2
        on stores_v2.p_end_date is null
        and stores_v2.store_subvertical2 = 'Groceries'
        and stores_v2.store_subvertical = 'QCPartners'
        and stores_v2.store_id = oc.store_id
    where 1=1
        and oc.p_creation_date in (select calendar_date from calendar_dates)
        and dynamic_session_id is not null
)

,order_events_metrics as (
    select
        --order created
        oc.p_creation_date
        ,oc.dynamic_session_id
        ,oc.customer_id
        ,oc.city
        ,oc.country
        ,oc.creation_time
        ,oc.platform
        --order descriptors
        ,od.order_id as od_order_id
        ,od.order_total_purchase_eur
        --retention
        ,roi.store_name_is_ret1
        ,roi.order_subvertical2_is_ret1
    from order_created oc
    left join delta.central_order_descriptors_odp.order_descriptors_v2 od
        on oc.oc_order_id = od.order_id
        and oc.p_creation_date = od.p_creation_date
        and od.order_final_status = 'DeliveredStatus' -- disclaimer
        and od.order_parent_relationship_type is null -- disclaimer
    left join delta.central__retention_orders__odp.retention_order_info roi
        on od.order_id = roi.order_id
        and od.p_creation_date = roi.p_creation_date
    where 1=1
        and oc.p_creation_date in (select calendar_date from calendar_dates)
        and dynamic_session_id is not null
        and oc.rn = 1 -- Order by p_creation_date and random() to break ties
)

,funnel as (
    select distinct -- because ce has multiple events per sessions, as well as order created
        -- category
        ce.p_creation_date
        ,ce.city
        ,ce.country
        ,ce.platform
        ,ce.dynamic_session_id as ce_dynamic_session_id
        -- top cities
        ,top_cities.n_stores_instore_price_enabled
        -- sessions
        ,ses.dynamic_session_id as ses_dynamic_session_id
        ,ses.is_recurrent_groceries
        -- order event metrics
        ,oe.dynamic_session_id as oe_dynamic_session_id
        ,oe.customer_id
        ,oe.od_order_id
        ,oe.order_total_purchase_eur
        ,oe.store_name_is_ret1
        ,oe.order_subvertical2_is_ret1
        -- variant
        ,ipg.variant
    from category_events ce
    inner join top_cities
        on ce.city = top_cities.order_city_code
    inner join delta.mfc__sessions_nc_rc__odp.sessions_nc_rc ses
        on ce.dynamic_session_id = ses.dynamic_session_id
    left join order_events_metrics oe
        on oe.dynamic_session_id = ce.dynamic_session_id
        and ce.p_creation_date = oe.p_creation_date
        and oe.creation_time between ce.creation_time and date_add('minute', 60, ce.creation_time)
    left join instore_prices_groups ipg
        on ipg.customer_id = ce.customer_id
        and ipg.start_time <= ce.creation_time
        and ipg.end_time > ce.creation_time
)

select
    -- dimensions
    p_creation_date
    ,city
    ,country
    ,platform
    ,n_stores_instore_price_enabled
    ,is_recurrent_groceries
    ,variant
    -- cvr
    ,count(distinct ce_dynamic_session_id) as ce_dynamic_session_id
    ,count(distinct oe_dynamic_session_id) as oe_dynamic_session_id
    -- orders
    ,count(distinct od_order_id) as n_orders
    ,count(distinct customer_id) as n_customers
    ,sum(order_total_purchase_eur) as sum_order_total_purchase_eur
    -- ret
    ,count(distinct case when store_name_is_ret1 then od_order_id else null end) as n_ret_orders_s
    ,count(distinct case when order_subvertical2_is_ret1 then od_order_id else null end) as n_ret_orders_g
from funnel
group by 1,2,3,4,5,6,7
