select
    count(*),
    count(distinct bought_product_id)
from delta.mfc__pna_gmv_variation__odp.products_gmv_variation