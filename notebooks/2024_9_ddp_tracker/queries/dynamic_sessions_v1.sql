with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date_add('day',-{days_in_advance},current_date),-- initial date
        date_add('day',-1,current_date),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,dynamic_sessions_v1 as (
    select 
        p_creation_date as p_creation_date 
        ,count(distinct dynamic_session_id) count_dynamic_session_id_sv_0_dynamic_sessions_v1
    from delta.customer_behaviour_odp.dynamic_sessions_v1
    where true
        and p_creation_date in (select calendar_date from calendar_dates)
        and count_sv__home > 0
    group by 1
)

select 
    *
from dynamic_sessions_v1