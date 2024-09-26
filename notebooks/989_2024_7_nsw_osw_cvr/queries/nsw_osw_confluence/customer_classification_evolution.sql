with calendar_dates as (select
    calendar_date
    from unnest(sequence(
        date({start_date}),
        date({end_date}),
        interval '1' day
    )) as dates (calendar_date)
    where true
)

,group_calendar_dates as (
    select
        calendar_date
    from unnest(sequence(
        date_add('day', -{days_between_start_date_and_first_exposure}, date({start_date})),
        date({end_date}),
        interval '1' day
    )) as dates (calendar_date)
    where true
)

,glovo_customers as (
    select
        u.user_id as customer_id
    from delta.central_users_odp.users_v2 u
    where true
        and not user_is_staff
        and not user_is_glovo_employee
        and user_type = 'Customer'
)

,customer_exposure as (
    select distinct
        fe.allocation_key as customer_id
        ,fe.variant
        ,date(fe.first_exposure_datetime) as start_date
        ,coalesce(lag(date(fe.first_exposure_datetime)) over (partition by fe.allocation_key order by date(fe.first_exposure_datetime) desc), date({end_date})) as end_date
    from delta.mlp__experiment_first_exposure__odp.first_exposure fe
    inner join glovo_customers gc
        on fe.allocation_key = gc.customer_id
    where true
        and fe.p_first_exposure_date in (select calendar_date from group_calendar_dates)
        and (fe.experiment_toggle_id = 'ZAP_CATEGORY_LANDING_PAGE' or fe.experiment_toggle_id = 'AP_CATEGORY_LANDING_PAGE_FOR_RETAIL')
)

select distinct
    variant,
    start_date,
    count(distinct customer_id) as n_distinct_customers
from customer_exposure
group by 1,2
order by 1,2