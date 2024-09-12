with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date_add('day',-{days_in_advance},current_date),-- initial date
        date_add('day',-1,current_date),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

select 
    'cart' as project_name
    ,date(creation_date) as p_creation_date
    ,event_name
    ,custom_attributes__product_added_origin as product_added_origin
    ,custom_attributes__product_selection_origin as product_selected_origin
    ,count(distinct event_id) as count_event
from sensitive_delta.customer_mpcustomer_odp.commerce_event 
where true
    and creation_date in (select calendar_date from calendar_dates)
    and (event_name = 'eCommerce - AddToCart')
    and custom_attributes__product_added_origin = 'Cart'
group by 1,2,3,4,5
order by 1 asc
