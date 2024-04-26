with products as (select
        trim(od.store_name) as store_name,
        od.order_city_code as order_city_code,
        bp.order_id as order_id,
        bp.product_external_id as external_id,
        bp.product_name as product_name,
        bp.bought_product_id as bought_product_id,
        bp.bought_product_quantity as quantity,
        bp.product_unit_price as product_unit_price
    from delta.customer_bought_products_odp.bought_products_v2 as bp
    left join delta.central_order_descriptors_odp.order_descriptors_v2 as od 
        on od.order_id = bp.order_id
        and date_format(od.p_creation_date , '%Y-%m-%d') = date_format(bp.p_creation_date , '%Y-%m-%d')
    where 1=1 
        and (od.order_final_status ) = 'DeliveredStatus' 
        and (od.order_country_code ) = 'PT' 
        --last 1 complete month
        and date(od.p_creation_date) >= date_add('month', -1, date_trunc('month', cast(date_trunc('day', now()) as date))) --lower bound
        and date(od.p_creation_date) < date_add('month', 1, date_add('month', -1, date_trunc('month', cast(date_trunc('day', now()) as date)))) --uper bound
)

,products_info as (
        select 
            bp1.bought_product_id,
            --has an addition
            case when 1=1
                and bp1.product_external_id = bp2.product_external_id
                and bp1.bought_product_quantity < bp2.bought_product_quantity
            then true else false end as has_an_addition,
            --has added quantity
            case when 1=1
                and bp1.product_external_id = bp2.product_external_id
                and bp1.bought_product_quantity < bp2.bought_product_quantity
            then bp2.bought_product_quantity - bp1.bought_product_quantity else null end as added_quantity
    from delta.customer_bought_products_odp.bought_products_v2 bp1
    left join delta.customer_bought_products_odp.bought_products_v2 bp2
        on bp1.order_id = bp2.order_id
        and bp1.replaced_by_bought_product_id = bp2.bought_product_id 
    where 1=1
        and bp1.order_id in (select order_id from products)
)

, products_with_additions as (
    select 
        *
        ,added_quantity*product_unit_price as added_gmv  
    from products p
    left join products_info pi
        on p.bought_product_id = pi.bought_product_id
    where 1=1
        and has_an_addition
)

select 
    store_name,
    order_city_code,
    product_name,   
    sum(added_quantity) as sum_added_quantity,
    sum(added_gmv) as sum_added_gmv
from products_with_additions
group by 1,2,3
order by 5 desc,4 desc