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
    ,custom_attributes__origin as origin
    ,custom_attributes__is_loyalty_card_added as is_loyalty_card_added
    -- ,'' as custom_attributes_3
    -- ,'' as custom_attributes_4
    -- ,'' as custom_attributes_5
    ,count(distinct event_id) as count_event
    ,count(distinct custom_attributes__store_id) as count_store_id
from sensitive_delta.customer_mpcustomer_odp.custom_event 
where true
    and creation_date in (select calendar_date from calendar_dates)
    and event_name = 'Store Membership Content Shown'
group by 1,2,3,4,5
order by 1 asc
