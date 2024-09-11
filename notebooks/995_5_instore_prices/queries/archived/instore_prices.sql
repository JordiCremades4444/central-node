with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date_add('day',-0,date({start_date})),-- initial date
        date_add('day',-0,date({end_date})),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,do_not_consider_groups as (
    select
        distinct data__experimentation_allocation_key as customer_id
    from delta.mlp_feature_store_experiment_exposure_odp.mlp_experiment_exposure
    where 1=1
        and p_event_date in (select calendar_date from calendar_dates)
        and data__experimentation_toggle_id in ('ZAP_CATEGORY_LANDING_PAGE', 'ZAP_NSW_EXPERIMENT')
        and data__experimentation_variant_value in (
            'forced_assignment' -- nsw exception
            ,'control_outofrange','forced_assignment_1','control_ft','control_ft' --instore prices experiment
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
        and allocation_key not in (select customer_id from do_not_consider_groups)
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
        and allocation_key not in (select customer_id from do_not_consider_groups)
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
        and customer_id not in(select customer_id from do_not_consider_groups)
)

,order_events as (
    select 
        p_creation_date,
        dynamic_session_id,
        event_id,
        customer_id,
        country,
        oc.creation_time,
        platform,
        category_id
    from delta.customer_behaviour_odp.enriched_backend_event__checkout_order_created_v3 oc
    inner join delta.partner_stores_odp.stores_v2
        on stores_v2.p_end_date is null
        and stores_v2.store_subvertical2 = 'Groceries'
        and stores_v2.store_id = oc.store_id
    where 1=1
        and p_creation_date in (select calendar_date from calendar_dates)
        and customer_id not in(select customer_id from do_not_consider_groups)
)

select 
    nswg.variant as nswg_variant,
    ipg.variant as ipg_variant,
    ce.p_creation_date,
    count(ce.event_id) as ce_events,
    count(oe.event_id) as oe_events
from category_events ce
left join order_events oe
    on oe.dynamic_session_id = ce.dynamic_session_id
    and oe.creation_time between ce.creation_time and date_add('minute', 120, ce.creation_time)
    and oe.category_id = ce.category_id
left join nsw_groups nswg
    on nswg.customer_id = ce.customer_id
    and nswg.start_time <= ce.creation_time
    and nswg.end_time > ce.creation_time
left join instore_prices_groups ipg
    on ipg.customer_id = ce.customer_id
    and ipg.start_time <= ce.creation_time
    and ipg.end_time > ce.creation_time
group by 1,2,3