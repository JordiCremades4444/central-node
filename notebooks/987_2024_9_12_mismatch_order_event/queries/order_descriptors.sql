select 
    p_creation_date
    ,count(distinct order_id) as count_order
from delta.central_order_descriptors_odp.order_descriptors_v2
where true
    and p_creation_date = date({day})
group by 1