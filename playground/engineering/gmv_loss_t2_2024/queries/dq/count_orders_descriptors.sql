select
    count(*),
    count(distinct order_id)
from delta.central_order_descriptors_odp.order_descriptors_v2 