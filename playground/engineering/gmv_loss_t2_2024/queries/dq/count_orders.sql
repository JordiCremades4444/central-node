select
    count(*),
    count(distinct order_id)
from delta.mfc__pna_gmv_variation__odp.orders_gmv_variation