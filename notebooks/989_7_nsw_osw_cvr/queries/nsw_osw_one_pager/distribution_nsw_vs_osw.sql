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

select 
    date(ng.start_time) as calendar_date
    ,ng.variant as variant
    ,count(distinct ng.customer_id) as distinct_customers --because an experiment can change multiple times during one day
from nsw_groups ng
where 1=1
group by 1,2
order by 1 asc