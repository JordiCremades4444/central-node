select
    dynamic_session_id,
    customer_id,
    creation_time,
    event_name,
    custom_attributes__widget_name,
    custom_attributes__origin
from sensitive_delta.customer_mpcustomer_odp.custom_event
where 1=1
    and dynamic_session_id = 'DB2C509C-FBF9-4A87-9827-17BB994BEDBB'
    and creation_date = date('2024-05-04')
order by creation_time asc