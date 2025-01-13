with calendar_dates as (
    select calendar_date
    from unnest(sequence(date({start_date}), date({end_date}), interval '1' day)) as cte (calendar_date)
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
    select
        date(fe.first_exposure_datetime) as p_creation_date
        ,fe.variant
        ,count(distinct fe.allocation_key) as n_customers
    from delta.mlp__experiment_first_exposure__odp.first_exposure fe
    inner join glovo_customers gc
        on fe.allocation_key = gc.customer_id
    inner join calendar_dates cd
        on cd.calendar_date = fe.p_first_exposure_date
    where true
         and (fe.experiment_toggle_id = 'ROCKET_PNA_DEFAULT_CHOICE_ET')
    group by 1,2
)

select * from customer_exposure

