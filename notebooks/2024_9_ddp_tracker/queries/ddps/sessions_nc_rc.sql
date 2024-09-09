with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date_add('day',-{days_in_advance},current_date),-- initial date
        date_add('day',-1,current_date),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,sessions_nc_rc as (
    select 
        p_creation_date
        ,count(distinct dynamic_session_id) count_dynamic_session_id_sessions_nc_rc
    from delta.mfc__sessions_nc_rc__odp.sessions_nc_rc
    where true 
        and p_creation_date in (select calendar_date from calendar_dates)
    group by 1
)

select 
    *
from sessions_nc_rc