select 
    tolce.p_creation_date
    ,cast(tolce.order_id as varchar) as order_id
    ,od.order_subvertical as order_subvertical
from delta.mfc__temp_order_loyalty_cards__odp.temp_order_loyalty_cards_enriched tolce
left join delta.central_order_descriptors_odp.order_descriptors_v2 od
    on tolce.order_id = od.order_id
where 1=1
    and tolce.p_creation_date = date({day})