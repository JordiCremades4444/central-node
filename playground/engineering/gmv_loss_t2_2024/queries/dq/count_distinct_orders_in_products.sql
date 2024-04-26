select
    count(distinct order_id)
from delta.mfc__pna_gmv_variation__odp.products_gmv_variation
limit 10