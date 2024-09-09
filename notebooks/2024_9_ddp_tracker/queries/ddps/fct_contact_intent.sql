with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date_add('day',-{days_in_advance},current_date),-- initial date
        date_add('day',-1,current_date),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,fct_contact_intent as (
    select 
        p_created_date as p_creation_date
        ,count(distinct feedback_id) count_feedback_id_fct_contact_intent
    from delta.contact_contact_intent_odp.fct_contact_intent
    where true
        and p_created_date in (select calendar_date from calendar_dates)
    group by 1
)

select 
    *
from fct_contact_intent