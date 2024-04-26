with calendar_dates as (select 
        calendar_date
    from unnest(
        sequence(
            date_add('day',-365,current_date)
            ,date_add('day',-2,current_date)
            ,interval '1' day
    )) as dates (calendar_date)
)

select 
    count(distinct order_id)
from delta.central_order_descriptors_odp.order_descriptors_v2 
where p_creation_date in (select calendar_date from calendar_dates)