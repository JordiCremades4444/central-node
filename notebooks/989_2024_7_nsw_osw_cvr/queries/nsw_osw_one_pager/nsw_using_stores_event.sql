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
        coalesce(lag(first_exposure_datetime) over (partition by allocation_key order by first_exposure_datetime desc), current_date) as end_time
    from delta.mlp__experiment_first_exposure__odp.first_exposure
    where 1=1
        and p_first_exposure_date in (select calendar_date from calendar_dates)
        and experiment_toggle_id = 'ZAP_CATEGORY_LANDING_PAGE' or experiment_toggle_id = 'ZAP_CATEGORY_LANDING_PAGE_FOR_RETAIL'
)

,cat_events as (
    select distinct
        date(p_creation_date) as p_creation_date
        ,upper(city) as city
        ,category_id
        ,category_tag
    from delta.customer_behaviour_odp.enriched_custom_event__category_opened_v3
    where 1=1
        --and category_tag in ('Groceries', 'Shops', 'Health', 'Food')
        and p_creation_date in (select calendar_date from calendar_dates)
)

select 
    date(s.p_creation_date)
    ,c.category_tag
    ,count(s.category_id) as store_wall_with_category_not_null
    ,count(distinct s.category_id) as store_wall_with_category_not_null
    ,count(s.event_id) as store_wall_events
    ,count(distinct s.event_id) as store_wall_distinct_events
from delta.customer_behaviour_odp.enriched_screen_view__stores_v3 s
left join cat_events c
    on s.category_id = c.category_id
    and s.city = c.city
    and s.p_creation_date = c.p_creation_date
where 1=1
    and s.p_creation_date in (select calendar_date from calendar_dates)
    and s.category_id is not null
group by 1,2
order by 1,2