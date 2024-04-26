with calendar_dates as (select 
        calendar_date
    from unnest(
        sequence(
            date_add('day',-30,current_date)
            ,date_add('day',-1,current_date)
            ,interval '1' day
    )) as dates (calendar_date)
)

,orders as (
    select distinct 
        order_id
from delta.central_order_descriptors_odp.order_descriptors_v2 
where 1=1
    and order_vertical = 'QCommerce'
    and order_subvertical = 'QCPartners'
    and order_subvertical2 = 'Groceries'
    and p_creation_date in (select * from calendar_dates)
)

,products_info as (
    select 
        bp1.*,
        case when 1=1
            and bp1.product_external_id = bp2.product_external_id
            and bp1.bought_product_quantity < bp2.bought_product_quantity
        then true else false
        end as has_an_addition,
        case when 1=1
            and bp1.product_external_id = bp2.product_external_id
            and bp1.bought_product_quantity < bp2.bought_product_quantity
        then bp2.bought_product_quantity - bp1.bought_product_quantity else null
        end as added_quantity
from delta.customer_bought_products_odp.bought_products_v2 bp1
left join delta.customer_bought_products_odp.bought_products_v2 bp2
    on bp1.order_id = bp2.order_id
    and bp1.replaced_by_bought_product_id = bp2.bought_product_id 
where 1=1
    and bp1.order_id in (select * from orders))

,products_with_additions as (
    select 
        *
    from products_info
    where 1=1
        and has_an_addition
)

select 
    count(distinct o.order_id) as n_orders,
    count(distinct pwa.order_id) as n_orders_with_additions
from orders o
left join products_with_additions pwa
    on o.order_id = pwa.order_id