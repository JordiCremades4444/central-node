with calendar_dates as (    
    select 
        calendar_date
    from unnest(
        sequence(
            date('2024-04-01') 
            ,current_date
            ,interval '1' day
    )) as dates (calendar_date)
)

,md_calendar as (    
    select 
        calendar_date
    from unnest(
        sequence(
            date('2024-05-02') 
            ,date('2024-05-05') 
            ,interval '1' day
    )) as dates (calendar_date)
)

,do_not_consider_customers as (
    select
        distinct allocation_key
    from delta.mlp__experiment_first_exposure__odp.first_exposure
    where 1=1
        and p_first_exposure_date in (select calendar_date from calendar_dates)
        and experiment_toggle_id = 'QC_HOME_STORE_WIDGET_ET'
        and variant = 'forced_assignment'
)

,variant_groups as (
    select
        allocation_key as customer_id, 
        variant as variant_group,
        p_first_exposure_date as start_period,
        coalesce(lag(p_first_exposure_date) over (partition by allocation_key order by first_exposure_datetime desc), current_date) as end_period
    from delta.mlp__experiment_first_exposure__odp.first_exposure
    where 1=1
        and p_first_exposure_date in (select calendar_date from calendar_dates)
        and experiment_toggle_id = 'QC_HOME_STORE_WIDGET_ET'
        and allocation_key not in (select allocation_key from do_not_consider_customers)
)

,retail_and_specialties_stores as (
    select distinct
        sa.store_address_id,
        sa.store_id
    from delta.partner_stores_odp.store_addresses_v2 sa
    left join delta.partner_stores_odp.stores_v2 s
        on sa.store_id = s.store_id
    where 1=1
        and (s.store_subvertical2 = 'Retail'
            or
            lower(s.store_subvertical) = 'qcpartners'
            and s.store_subvertical2 = 'Groceries'
            and s.store_sub_business_unit NOT IN ('Convenience', 'Supermarket', 'Other', 'Fake','CONVENIENCE', 'OTHER', 'SUPERMARKET', 'FAKE')
            ) --retail or specialties
        and date(s.start_date) <= (select min(calendar_date) from md_calendar)
        and date(sa.p_end_date) <= (select min(calendar_date) from md_calendar)
)

,home_sessions as (
    select distinct 
        dynamic_session_id,
        first_value(customer_id) over (partition by dynamic_session_id order by creation_time_local asc) as customer_id,
        first_value(p_creation_date) over (partition by dynamic_session_id order by p_creation_date asc) as p_creation_date,
        first_value(country) over (partition by dynamic_session_id order by creation_time_local asc) as country,
        first_value(creation_time_local) over (partition by dynamic_session_id order by creation_time_local asc) as creation_time_local
    from delta.customer_behaviour_odp.enriched_screen_view__home_v3
    where 1=1
        and country is not null
        and customer_id is not null
        and p_creation_date in (select calendar_date from calendar_dates)
)

,orders_created as (
    select distinct
        dynamic_session_id,
        order_id,
        first_value(customer_id) over (partition by dynamic_session_id order by creation_time_local asc) as customer_id,
        first_value(p_creation_date) over (partition by dynamic_session_id order by p_creation_date asc) as p_creation_date,
        first_value(country) over (partition by dynamic_session_id order by creation_time_local asc) as country,
        first_value(creation_time_local) over (partition by dynamic_session_id order by creation_time_local asc) as creation_time_local
    from delta.customer_behaviour_odp.enriched_custom_event__order_created_v3 sa
    inner join retail_and_specialties_stores 
        on retail_and_specialties_stores.store_address_id = sa.store_address_id
    where 1=1
        and country is not null
        and customer_id is not null
        and p_creation_date in (select calendar_date from calendar_dates)
)

select --Careful we are not comparing sessions to sessions. So it is a ratio, not a percentage
    hs.p_creation_date,
    hs.country,
    vg.variant_group,
    count(distinct hs.dynamic_session_id) as n_home_sessions,
    count(distinct oc.order_id) as n_orders
from home_sessions hs
left join orders_created oc
    on oc.dynamic_session_id = hs.dynamic_session_id
    and oc.creation_time_local > hs.creation_time_local
left join variant_groups vg
    on vg.customer_id = hs.customer_id
    and hs.p_creation_date between vg.start_period and vg.end_period
where 1=1
    and hs.p_creation_date in (select calendar_date from md_calendar)
group by 1,2,3
order by 1,2,3
