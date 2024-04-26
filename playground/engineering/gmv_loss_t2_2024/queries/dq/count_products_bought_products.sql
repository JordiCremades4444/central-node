select
    count(*),
    count(distinct bought_product_id)
from delta.customer_bought_products_odp.bought_products_v2