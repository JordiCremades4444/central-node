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
    'loyalty cards' as project_name
    ,date(creation_date) as p_creation_date
    ,event_name
    ,custom_attributes__tooltip_type as tooltip_type
    -- ,'' as custom_attributes_2
    -- ,'' as custom_attributes_3
    -- ,'' as custom_attributes_4
    -- ,'' as custom_attributes_5
    ,count(distinct event_id) as count_event
from sensitive_delta.customer_mpcustomer_odp.custom_event 
where true
    and creation_date in (select calendar_date from calendar_dates)
    and event_name = 'Tooltip Impression'
group by 1,2,3,4
order by 1 asc