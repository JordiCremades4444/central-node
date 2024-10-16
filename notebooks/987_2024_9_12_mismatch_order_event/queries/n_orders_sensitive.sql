select 
    creation_date
    ,count(distinct custom_attributes__order_id) as count_order_sensitive
from sensitive_delta.customer_mpcustomer_odp.custom_event 
where true
    and creation_date = date({day})
    and event_name = 'Order Created'
group by 1