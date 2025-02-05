with calendar_dates as (select
    calendar_date
    from unnest(sequence(date({start_date}),date({end_date}),interval '1' day)) as dates (calendar_date)
    where true
)

,sessions as (
    select
        p_creation_date as creation_date,
        count(distinct h.dynamic_session_id) as n_sessions
    from delta.customer_behaviour_odp.enriched_screen_view__home_v3 h
    inner join calendar_dates
        on h.p_creation_date = calendar_dates.calendar_date
    where true
    group by 1
)

,taps as (
    select
        creation_date,
        count(case when cu.custom_attributes__collection_id is not null then cu.event_id end) as n_collection_taps_session,
        count(case when cu.custom_attributes__collection_group_id is not null then cu.event_id end) as n_supercollection_taps_session,
        count(case when cu.custom_attributes__collection_id is not null or cu.custom_attributes__collection_group_id is not null then cu.event_id end) as n_total_taps_session
    from sensitive_delta.customer_mpcustomer_odp.custom_event cu
    inner join calendar_dates
        on cu.creation_date = calendar_dates.calendar_date
    where true
        and cu.event_name = 'Navigation Bar Element Tapped'
        and (custom_attributes__collection_type = 'Catalogue' or custom_attributes__collection_type is null)
    group by 1
)

,evolution as (
    select  
        h.creation_date,
        h.n_sessions,
        t.n_collection_taps_session,
        t.n_supercollection_taps_session,
        t.n_total_taps_session
    from sessions h
    left join taps t
        on h.creation_date = t.creation_date
)

select 
    1.000*sum(n_collection_taps_session)/sum(n_sessions) as collection_taps_per_session,
    1.000*sum(n_supercollection_taps_session)/sum(n_sessions) as supercollection_taps_per_session,
    1.000*sum(n_total_taps_session)/sum(n_sessions) as total_taps_per_session 
from evolution