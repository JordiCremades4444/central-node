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
            else 'Out of Experiment SW' end exp_group_nsw
    from delta.mlp_feature_store_experiment_exposure_odp.mlp_experiment_exposure
    where 1=1
        and data__experimentation_toggle_id = ({toggle_nsw})
        and p_event_date in (select calendar_date from calendar_dates)
        and not p_event_date in (select calendar_date from sv_calendar_dates)
)

,instore_groups as ( --Groups for instore price
    select distinct 
        p_event_date,
        data__experimentation_allocation_key as customer_id,
        case 
            when data__experimentation_variant_value = 'Control Group' then 'Control Inprice'
            when data__experimentation_variant_value = 'InStorePrices' THEN 'Variant Inprice'
            else 'Out of Experiment Inprices' end exp_group_instore
    from delta.mlp_feature_store_experiment_exposure_odp.mlp_experiment_exposure
    where 1=1
        and data__experimentation_toggle_id = ({toggle_instore})
        and p_event_date in (select calendar_date from calendar_dates)
        and not p_event_date in (select calendar_date from sv_calendar_dates)
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

,all_funnel as (
    select
        swe.p_creation_date,
        swe.country as country,
        swe.platform as platform,
        coalesce(exp_group_nsw, 'Not Found SW') as exp_group_nsw,
        coalesce(exp_group_instore, 'Not Found Instore') as exp_group_instore,
        category_id,
        -- measures
        count(distinct swe.dynamic_session_id) as count_swe_dynamic_session_id
    from store_walls_events as swe --store wall
    left join nsw_groups
        on swe.customer_id = nsw_groups.customer_id
        and swe.p_creation_date = nsw_groups.p_event_date
    left join instore_groups
        on swe.customer_id = instore_groups.customer_id
        and swe.p_creation_date = instore_groups.p_event_date
    group by 1,2,3,4,5,6
)

select 
    * 
from all_funnel
order by 1,2,3,4,5
