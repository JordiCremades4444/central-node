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
    and bp1.order_id in (100308613825,596405134,628322096,100382450564)