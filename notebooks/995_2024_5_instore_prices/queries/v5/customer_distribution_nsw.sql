with calendar_dates as (select
        calendar_date
    from unnest(sequence(date({start_date}),date({end_date}),interval '1' day)) as dates (calendar_date)
    where 1=1
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
)

,category_events as (
    select 
        p_creation_date,
        dynamic_session_id,
        event_id,
        customer_id,
        country,
        creation_time,
        platform
    from delta.customer_behaviour_odp.enriched_custom_event__category_opened_v3
    where 1=1
        and p_creation_date in (select calendar_date from calendar_dates)
        and category_tag = 'Groceries'
)

select 
    nswg.variant as nswg_variant,
    ce.p_creation_date,
    count(ce.customer_id) as ce_events
from category_events ce
left join nsw_groups nswg
    on nswg.customer_id = ce.customer_id
    and nswg.start_time <= ce.creation_time
    and nswg.end_time > ce.creation_time
group by 1,2
