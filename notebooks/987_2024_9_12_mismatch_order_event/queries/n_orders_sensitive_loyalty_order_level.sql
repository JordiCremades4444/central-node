select 
    creation_date as p_creation_date
    ,cast(custom_attributes__order_id as varchar) as order_id
    ,custom_attributes__is_loyalty_card_added as is_loyalty_card_added
from sensitive_delta.customer_mpcustomer_odp.custom_event 
where true
    and creation_date = date({day})
    and event_name = 'Order Created'
    and custom_attributes__is_loyalty_card_added = 'true'