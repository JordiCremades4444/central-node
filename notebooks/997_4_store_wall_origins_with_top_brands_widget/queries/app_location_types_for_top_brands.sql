with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date_add('day',-30,current_date),-- initial date
        date_add('day',-1,current_date),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

select 
    custom_attributes__widget_app_location,
    count(*)
from sensitive_delta.customer_mpcustomer_odp.custom_event
where 1=1
    and creation_date in (select calendar_date from calendar_dates)
    and event_name = 'Widget Interacted'
    and custom_attributes__widget_name = 'TOP_BRANDS'
group by 1