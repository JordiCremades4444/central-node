with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date_add('day',-0,date({start_date})),-- initial date
        date_add('day',-0,date({end_date})),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,do_not_consider_nsw_groups as (
    select
        distinct data__experimentation_allocation_key as customer_id
    from delta.mlp_feature_store_experiment_exposure_odp.mlp_experiment_exposure
    where 1=1
        and p_event_date in (select calendar_date from calendar_dates)
        and data__experimentation_toggle_id = 'ZAP_CATEGORY_LANDING_PAGE'
        and data__experimentation_variant_value = 'forced_assignment' 
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
        and allocation_key not in (select customer_id from do_not_consider_nsw_groups)
)

,category_events as (
    select 
        p_creation_date,
        dynamic_session_id,
        event_id,
        customer_id,
        country,
        creation_time,
        platform,
        category_id
    from delta.customer_behaviour_odp.enriched_custom_event__category_opened_v3
    where 1=1
        and p_creation_date in (select calendar_date from calendar_dates)
        and category_id in ({list_category_id})
        and customer_id not in(select customer_id from do_not_consider_nsw_groups)
)

,store_walls_events as (
    select 
        p_creation_date,
        dynamic_session_id,
        event_id,
        customer_id,
        country,
        creation_time,
        platform,
        category_id
    from delta.customer_behaviour_odp.enriched_screen_view__stores_v3
    where 1=1  
        and p_creation_date in (select calendar_date from calendar_dates)
        and category_id in ({list_category_id})
        and customer_id not in(select customer_id from do_not_consider_nsw_groups)
)

,order_events as (
    select 
        p_creation_date,
        dynamic_session_id,
        event_id,
        customer_id,
        country,
        creation_time,
        platform,
        category_id
    from delta.customer_behaviour_odp.enriched_backend_event__checkout_order_created_v3
    where 1=1
        and p_creation_date in (select calendar_date from calendar_dates)
        and customer_id not in(select customer_id from do_not_consider_nsw_groups)
)

select 
    nswg.variant,
    ce.p_creation_date,
    count(ce.event_id) as ce_events,
    count(swe.event_id) as swe_events,
    count(oe.event_id) as oe_events
from category_events ce
left join store_walls_events swe
    on swe.dynamic_session_id = ce.dynamic_session_id
    and swe.creation_time between ce.creation_time and date_add('minute', 1, ce.creation_time)
    and swe.category_id = ce.category_id
left join order_events oe
    on oe.dynamic_session_id = swe.dynamic_session_id
    and oe.creation_time between swe.creation_time and date_add('minute', 120, swe.creation_time)
    and oe.category_id = swe.category_id
left join nsw_groups nswg
    on nswg.customer_id = ce.customer_id
    and nswg.start_time <= ce.creation_time
    and nswg.end_time > ce.creation_time
group by 1,2