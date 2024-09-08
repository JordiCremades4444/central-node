with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date_add('day',-{days_in_advance},current_date),-- initial date
        date_add('day',-1,current_date),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,groceries_top_partners as (
    select 
        p_ingestion_date as p_creation_date
        ,count(*) count_store_names_countries_groceries_top_partners
    from delta.mfc__groceries_content_availability_targets__odp.groceries_top_partners
    where true
        and p_ingestion_date in (select calendar_date from calendar_dates)
    group by 1
)

select 
    *
from groceries_top_partners