with calendar_dates as (
    select 
        calendar_date
    from unnest(sequence(date({start_date}), date({end_date}), interval '1' day)) as cte (calendar_date)
    where true 
)

,group_calendar_dates as (
    select
        calendar_date
    from unnest(sequence(date_add('day', -60, date({start_date})),date({end_date}),interval '1' day)) as dates (calendar_date)
    where true
)

,customers_glovo as (
    select
        u.user_id as customer_id
    from delta.central_users_odp.users_v2 u
    where true
        and not user_is_staff
        and not user_is_glovo_employee
        and user_type = 'Customer'
)

,stores as (
    select distinct
        sa.store_address_id,
        sa.store_id,
        s.store_name
    from delta.partner_stores_odp.store_addresses_v2 sa
    left join delta.partner_stores_odp.stores_v2 s
        on sa.store_id = s.store_id
    left join delta.central_geography_odp.cities_v2 c
        on s.city_code = c.city_code
    where true
        and sa.p_end_date is null
        and s.p_end_date is null
        and s.store_subvertical = 'QCPartners'
        and s.store_subvertical2 = 'Groceries'
        and sa.store_address_id in ({sad_ids}) --eataly and penny partners
)

,orders_gmv_loss as (
    select
        *
    from delta.mfc__pna_gmv_variation__odp.orders_gmv_variation
    where true
        and p_creation_date in (select calendar_date from calendar_dates)
        and store_address_id in (select store_address_id from stores)
        and customer_id in (select customer_id from customers_glovo)
)

select
    *
from orders_gmv_loss