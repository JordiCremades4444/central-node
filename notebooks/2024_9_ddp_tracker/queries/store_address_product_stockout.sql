with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date_add('day',-{days_in_advance},current_date),-- initial date
        date_add('day',-1,current_date),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,store_address_product_stockout as (
    select 
        p_event_date as p_creation_date 
        ,count(distinct store_address_id) count_is_stock_out_integration_store_address_product_stockout
    from delta.partner_lifecycle_odp.store_address_product_stockout 
    where true
        and p_event_date in (select calendar_date from calendar_dates)
        and event_source = 'INTEGRATION'
        and is_stock_out
    group by 1
)

select 
    *
from store_address_product_stockout