with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date_add('day',-{days_in_advance},current_date),-- initial date
        date_add('day',-1,current_date),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,active_partners as (
    select 
        p_run_date as p_creation_date
        ,count(distinct store_address_id) count_store_address_id_enabled_active_partners
    from delta.partner_lifecycle_odp.active_partners
    where true
        and p_run_date in (select calendar_date from calendar_dates)
    group by 1
)

select 
    *
from active_partners