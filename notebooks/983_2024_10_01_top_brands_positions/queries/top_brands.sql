with calendar_dates as (select
    calendar_date
    from unnest(sequence(
        date({start_date}),
        date({end_date}),
        interval '1' day
    )) as dates (calendar_date)
    where true
)

,map_category_opened as ( -- cateogy end possibilities are Food, Health, Groceries, Shops, Smoking and Specialties
    select distinct
        co.p_creation_date,
        co.country,
        co.category_id,
        case when co.category_sub_tag in ('Smoking', 'Specialties') then co.category_sub_tag else co.category_tag end as category
    from delta.customer_behaviour_odp.enriched_custom_event__category_opened_v3 co
    inner join calendar_dates
        on co.p_creation_date = calendar_dates.calendar_date
    where 1=1
        and co.category_tag in ('Food', 'Health', 'Groceries', 'Shops')
        and country is not null
        and category_id is not null
)

select
    si.p_creation_date,
    si.country,
    mco.category,
    si.widget_horizontal_position,
    si.widget_app_location,
    si.origin,
    si.widget_name,
    si.widget_id,
    si.store_id,
    si.store_name,
    count(distinct si.event_id) n_events_wsi,
    count(distinct sa.event_id) n_events_sa,
    count(distinct oc.event_id) n_events_oc
from delta.customer_behaviour_odp.enriched_custom_event__store_impression_v3 si
inner join calendar_dates
    on si.p_creation_date = calendar_dates.calendar_date
left join map_category_opened mco
    on si.p_creation_date = mco.p_creation_date
    and si.country = mco.country
    and si.category_id = mco.category_id 
left join delta.customer_behaviour_odp.enriched_custom_event__store_accessed_v3 sa
    on si.p_creation_date = sa.p_creation_date
    and si.creation_time <= sa.creation_time
    and si.dynamic_session_id = sa.dynamic_session_id
    and si.store_id = sa.store_id
left join delta.customer_behaviour_odp.enriched_custom_event__order_created_v3 oc
    on sa.p_creation_date = oc.p_creation_date
    and sa.creation_time <= oc.creation_time
    and sa.dynamic_session_id = oc.dynamic_session_id
    and sa.store_id = oc.store_id
where true 
    and si.origin in ('Widget','WidgetResults')
    and si.category_id is not null
    and si.country is not null
    and si.widget_name = 'TOP_BRANDS'
group by 1,2,3,4,5,6,7,8,9,10