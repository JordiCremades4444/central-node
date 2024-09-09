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
    'limiting quantities' as project_name
    ,date(creation_date) as p_creation_date
    ,event_name
    ,custom_attributes__snack_bar_type as snackbar_type
    ,custom_attributes__quantity_limit_logic as quantity_limit_logic
    ,custom_attributes__snack_bar_screen as snackbar_screen
    ,count(distinct event_id) as count_event
    ,count(distinct custom_attributes__store_id) as count_store_id
    ,count(distinct custom_attributes__store_address_id) as count_store_address_id
    ,count(distinct custom_attributes__product_id) as count_product_id
    ,count(distinct custom_attributes__store_product_id) as count_store_product_id
from sensitive_delta.customer_mpcustomer_odp.custom_event 
where true
    and creation_date in (select calendar_date from calendar_dates)
    and event_name = 'Snackbar Shown'
group by 1,2,3,4,5,6
order by 1 asc
