select
    store_id,
    store_name
from delta.partner_stores_odp.stores_v2
where 1=1
    and p_end_date is null
    and store_id in ({cities})