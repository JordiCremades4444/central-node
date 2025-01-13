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
    select distinct
        fe.allocation_key as customer_id
        ,fe.variant
        ,fe.first_exposure_datetime as start_time
        ,coalesce(lag(fe.first_exposure_datetime) over (partition by fe.allocation_key order by fe.first_exposure_datetime desc), current_timestamp) as end_time
    from delta.mlp__experiment_first_exposure__odp.first_exposure fe
    inner join glovo_customers gc
        on fe.allocation_key = gc.customer_id
    inner join calendar_dates cd
        on cd.calendar_date = fe.p_first_exposure_date
    where true
         and (fe.experiment_toggle_id = 'ROCKET_PNA_DEFAULT_CHOICE_ET')
)

select distinct
    od.p_creation_date,
    od.order_id,
    cast(od.customer_id as varchar) as customer_id,
    ce.variant
from delta.central_order_descriptors_odp.order_descriptors_v2 od 
inner join calendar_dates cd
    on od.p_creation_date = cd.calendar_date
left join customer_exposure ce
    on od.customer_id = ce.customer_id
    and od.order_created_at between ce.start_time and ce.end_time
where true
    and od.store_address_id in ({store_addresses_id})
