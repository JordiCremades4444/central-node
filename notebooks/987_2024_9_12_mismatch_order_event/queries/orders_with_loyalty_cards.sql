select 
    p_creation_date
    ,count(distinct order_id) as count_order
from delta.mfc__temp_order_loyalty_cards__odp.temp_order_loyalty_cards_enriched 
where 1=1
    and p_creation_date = date({day})
group by 1