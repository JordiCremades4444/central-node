with calendar_dates as (
    select
        calendar_date
    from unnest(sequence(date({start_date}),date({end_date}),interval '1' day)) as dates (calendar_date)
    where 1=1
)

, groups_calendar_dates as (
    select
        calendar_date
    from unnest(sequence(date_add('day',-90,date({start_date})),date({end_date}),interval '1' day)) as dates (calendar_date)
    where 1=1
)

,nsw_groups as (
    select
        allocation_key as customer_id,
        variant,
        first_exposure_datetime as start_time,
        coalesce(lag(first_exposure_datetime) over (partition by allocation_key order by first_exposure_datetime desc), current_date) as end_time
    from delta.mlp__experiment_first_exposure__odp.first_exposure
    where 1=1
        and p_first_exposure_date in (select calendar_date from groups_calendar_dates)
        and experiment_toggle_id = 'ZAP_CATEGORY_LANDING_PAGE' or experiment_toggle_id = 'ZAP_CATEGORY_LANDING_PAGE_FOR_RETAIL'
)

,cat_events as (
    select distinct
        date(p_creation_date) as p_creation_date,
        coalesce(nsw.variant,'not_found_variant') as variant,
        count(distinct c.customer_id) as n_customers
    from delta.customer_behaviour_odp.enriched_custom_event__category_opened_v3 c
    left join nsw_groups nsw
        on c.customer_id = nsw.customer_id
        and c.creation_time >= nsw.start_time
        and c.creation_time < nsw.end_time
    where 1=1
        and category_tag in ('Groceries', 'Shops', 'Health')
        and p_creation_date in (select calendar_date from calendar_dates)
    group by 1,2
)

select 
    *
from cat_events
order by 1 asc