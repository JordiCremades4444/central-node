with calendar_dates as (
    select
        calendar_date
    from unnest(sequence(date({start_date}),date({end_date}),interval '1' day)) as dates (calendar_date)
    where 1=1
)

,lc_groups as (
    select
        allocation_key as customer_id,
        variant,
        first_exposure_datetime as start_time,
        coalesce(lag(first_exposure_datetime) over (partition by allocation_key order by first_exposure_datetime desc), current_date) as end_time
    from delta.mlp__experiment_first_exposure__odp.first_exposure
    where 1=1
        and p_first_exposure_date in (select calendar_date from calendar_dates)
        and experiment_toggle_id = 'SONIC_LOYALTY_CARDS_NEW'
)

select
    date(start_time) as date,
    variant,
    count(distinct customer_id) as n_first_exposure_customers
from lc_groups
group by 1,2
order by 1 asc