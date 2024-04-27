with calendar_dates as (    
    select 
        calendar_date
    from unnest(
        sequence(
            date_add('day',0,date({start_experiment})) 
            ,date_add('day',0,date({end_experiment}))
            ,interval '1' day
    )) as dates (calendar_date)
)

,sv_calendar_dates as (-- Dates for SV event, when all experiments share the same SALT
    select 
        calendar_date
    from unnest(
        sequence(
            date_add('day',0,date({sv_start_experiment})) 
            ,date_add('day',0,date({sv_end_experiment}))
            ,interval '1' day
    )) as dates (calendar_date)
)

,nsw_groups as ( --Groups for NSW
    select distinct
        p_event_date as p_event_date,
        data__experimentation_allocation_key as customer_id,
        case 
            when data__experimentation_variant_value = 'Control Group' then 'Control SW'
            when data__experimentation_variant_value = 'Variant 1' THEN 'Variant SW'
            else 'Out of Experiment' end exp_group_nsw
    from delta.mlp_feature_store_experiment_exposure_odp.mlp_experiment_exposure
    where 1=1
        and data__experimentation_toggle_id = ({toggle_nsw})
        and p_event_date in (select calendar_date from calendar_dates)
        and not p_event_date in (select calendar_date from sv_calendar_dates)
)

,category_events as ( --Sessions that reach a category id XXX
    select distinct
        p_creation_date,
        dynamic_session_id,
        event_id,
        customer_id,
        country,
        creation_time,
        platform,
        category_id
    from delta.customer_behaviour_odp.enriched_custom_event__category_opened_v3
    where 1=1
        and p_creation_date in (select calendar_date from calendar_dates)
        and category_id in ({list_category_id})
)

,store_walls_events as ( --Sessions that reach the store wall. We need a union to solve an issue with data
    select distinct
        p_creation_date,
        dynamic_session_id,
        event_id,
        customer_id,
        country,
        creation_time,
        platform,
        category_id
    from delta.customer_behaviour_odp.enriched_screen_view__stores_v3
    where 1=1
        and p_creation_date in (select calendar_date from calendar_dates)
        and category_id in ({list_category_id})
    
    union -- needed to recover data 
    
    select distinct
       creation_date as p_creation_date,
       dynamic_session_id,
       cast(event_id as bigint) as event_id,
       customer_id,
       custom_attributes__country as country,
       creation_time,
       device_info__platform as platform,
       cast(custom_attributes__category_id as bigint) as category_id
    from sensitive_delta.customer_mpcustomer_odp.custom_event --"legacy_delta"."refinery_mpcustomer"."custom_event"
    where 1=1
        and creation_date in (select calendar_date from calendar_dates)
        and cast(custom_attributes__category_id as bigint) in ({list_category_id})
        and event_name = 'Stores'
)

,store_events as ( --Sessions that reach a store impressions
    select distinct
        scv.p_creation_date,
        scv.dynamic_session_id,
        scv.event_id,
        scv.customer_id,
        scv.country,
        scv.creation_time,
        scv.platform,
        scv.store_id,
        scv.store_address_id,
        s.store_sub_business_unit,
        s.store_subvertical,
        case when (
                s.store_subvertical <> 'MFC' 
                and s.store_subvertical3 = 'Groceries' 
                and upper(s.store_sub_business_unit) not in ('CONVENIENCE', 'SUPERMARKET', 'OTHER')
            ) then true else false end as is_specialties
    from delta.customer_behaviour_odp.enriched_screen_view__store_v3 as scv
    inner join delta.partner_stores_odp.stores_v2 as s
        on s.p_end_date is null
        and s.store_subvertical2 = 'Groceries'
        and s.store_id = scv.store_id
    where 1=1
        and p_creation_date in (select calendar_date from calendar_dates)
)

,order_events as ( --Sessions that reach order event
    select 
        p_creation_date,
        dynamic_session_id,
        event_id,
        customer_id,
        country,
        creation_time,
        platform,
        store_id,
        store_address_id
    from delta.customer_behaviour_odp.enriched_backend_event__checkout_order_created_v3
    where 1=1
        and p_creation_date in (select calendar_date from calendar_dates)
)

,all_funnel as (
    select
        ce.p_creation_date,
        upper(coalesce(ce.country, se.country, oe.country)) as country,
        upper(coalesce(ce.platform, se.platform, oe.platform)) as platform,
        coalesce(exp_group_nsw, 'Not Found') as exp_group_nsw,
        -- measures
        count(distinct ce.dynamic_session_id) as count_ce_dynamic_session_id,
        count(distinct swe.dynamic_session_id) as count_swe_dynamic_session_id,        
        count(distinct se.dynamic_session_id) as count_se_dynamic_session_id,
        count(distinct oe.dynamic_session_id) as count_oe_dynamic_session_id
    from category_events as ce --category events
    left join store_walls_events as swe --store wall
        on swe.p_creation_date between ce.p_creation_date and date_add('day', 2, ce.p_creation_date)
        and swe.creation_time between ce.creation_time and date_add('minute', 1, ce.creation_time)
        and swe.dynamic_session_id = ce.dynamic_session_id
        and swe.category_id = ce.category_id 
    left join store_events as se --store impressions
        on se.p_creation_date between ce.p_creation_date and date_add('day', 2, ce.p_creation_date)
        and se.creation_time between swe.creation_time and date_add('minute', 120, swe.creation_time)
        and ce.dynamic_session_id = se.dynamic_session_id
    left join order_events as oe --order events
        on oe.p_creation_date between ce.p_creation_date and date_add('day', 2, ce.p_creation_date)
        and oe.creation_time between se.creation_time and date_add('minute', 120, se.creation_time)
        and oe.dynamic_session_id = se.dynamic_session_id
        and oe.store_address_id = se.store_address_id
    left join nsw_groups
        on coalesce(ce.customer_id, se.customer_id, oe.customer_id) = nsw_groups.customer_id
        and ce.p_creation_date = nsw_groups.p_event_date
    group by 1,2,3,4
)

select 
    * 
from all_funnel
order by 1,2,3,4,5
