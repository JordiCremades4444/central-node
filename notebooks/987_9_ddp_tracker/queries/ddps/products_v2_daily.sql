with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date_add('day',-{days_in_advance},current_date),-- initial date
        date_add('day',-1,current_date),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)
,products_v2_daily as (
    select 
        p_extraction_date as p_creation_date
        ,count(*) count_products_v2_daily
    from delta.mfc_dh_curated_odp.products_v2_daily
    where true
        and p_extraction_date in (select calendar_date from calendar_dates)
    group by 1
)

select 
    *
from products_v2_daily