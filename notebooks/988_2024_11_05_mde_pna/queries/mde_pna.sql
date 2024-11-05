with calendar_dates as (select
        calendar_date
    from unnest(sequence(date('2024-10-01'), date('2024-11-01'), interval '1' day)) as dates (calendar_date)
    where true
)

,group_calendar_dates as (select
        calendar_date
    from unnest(sequence(date_add('day',-{buffer},date('2024-10-01')),date('2024-11-01'), interval '1' day)) as dates (calendar_date)
    
)

,do_not_consider_customers as (
    select
        user_id as customer_id
    from delta.central_users_odp.users_v2
    where true
        and not user_is_staff
        and not user_is_glovo_employee
        and user_type = 'Customer'
)

,stores as (
    select distinct
        sa.store_address_id
    from delta.partner_stores_odp.store_addresses_v2 sa
    left join delta.partner_stores_odp.stores_v2 s
        on sa.store_id = s.store_id
    left join delta.central_geography_odp.cities_v2 c
        on s.city_code = c.city_code
    where true
        and sa.p_end_date is null
        and s.p_end_date is null
        and s.store_name = 'PENNY.'
        and c.country_code = 'IT'
)

,customers_that_bought_in_penny as (
    select
        od.customer_id,
        count(distinct od.order_id) as orders,
        count(distinct case when bp.replaced_bought_product_id is null then bp.bought_product_id else null end) as products
    from delta.customer_bought_products_odp.bought_products_v2 bp
    inner join calendar_dates cd
        on bp.p_creation_date = cd.calendar_date
    inner join delta.central_order_descriptors_odp.order_descriptors_v2 od
        on bp.order_id = od.order_id
        and bp.p_creation_date = od.p_creation_date
    inner join stores s
        on s.store_address_id = bp.store_address_id
    where true
    group by 1
)

select * from customers_that_bought_in_penny pen
