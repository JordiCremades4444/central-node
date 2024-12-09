with calendar_dates as (
    select
        calendar_date
    from unnest(sequence(date{start_date}, date{end_date}, interval '1' day)) as cte (calendar_date)
    where true
)

,glovo_customers as (
    select
        u.user_id as customer_id
    from delta.central_users_odp.users_v2 u
    where true
        and not user_is_staff
        and not user_is_glovo_employee
        and user_type = 'Customer'
)

,stores as (
    select distinct
        sa.store_address_id,
        sa.store_id
    from delta.partner_stores_odp.store_addresses_v2 sa
    left join delta.partner_stores_odp.stores_v2 s
        on sa.store_id = s.store_id
    where true
        and sa.p_end_date is null
        and s.p_end_date is null
        and s.store_subvertical2 = 'Groceries'
)

,all_exposures as (
    select 
        variant,
        allocation_key as customer_id,
        min(first_exposure_datetime) as first_exposure_datetime,
        min(p_first_exposure_date) as first_exposure_date
    from delta.mlp__experiment_first_exposure__odp.first_exposure as e
    inner join calendar_dates cd
        on cd.calendar_date = e.p_first_exposure_date
    inner join glovo_customers gc
        on gc.customer_id = e.allocation_key
    where true
        and experiment_toggle_id = 'ZAP_SURFACING_PROMOS'
    group by 1,2
)

,groceries_store_accessed_surfacing_promos as (
    select
        sa.dynamic_session_id,
        sa.p_creation_date,
        ae.customer_id,
        ae.variant,
        sa.p_creation_date as event_date,
        concat(cast(sa.customer_id as varchar), cast(sa.dynamic_session_id as varchar), cast(sa.session_start_time as varchar)) as pk_session_id,
        row_number() over (partition by sa.customer_id order by sa.creation_time asc) as session_rn
    from delta.customer_behaviour_odp.enriched_custom_event__store_accessed_v3 AS sa
    inner join calendar_dates cd
        on cd.calendar_date = sa.p_creation_date
    inner join all_exposures ae
        on sa.customer_id = ae.customer_id
        and sa.creation_time >= ae.first_exposure_datetime --store access should happen after customer is on a variant
    inner join stores s
        on s.store_id = sa.store_id
    where true
        and not (cardinality(promotion_ids) = 1 and contains(promotion_ids, -1)) --exclude the promotion_id -1 if appears alone
        and promotion_ids is not null
)

,fixed_exposures as (
    select 
        customer_id,
        variant,
        min(event_date) as first_exposure_at
    from groceries_store_accessed_surfacing_promos 
    where true
        and session_rn = 1 --de cada session em quedo amb la primera entrada
    group by 1,2
)

,banner_impression__shoppable as (
    select
        bi.customer_id,
        count(distinct bi.event_id) as banner_impressions,
        count(distinct concat(cast(bi.dynamic_session_id as varchar), cast(bi.banner_id as varchar))) as distinct_banner_impressions_per_session
    from delta.customer_behaviour_odp.enriched_custom_event__banner_impression_v3 bi
    inner join calendar_dates
        on bi.p_creation_date = calendar_dates.calendar_date
    inner join stores
        on stores.store_address_id = bi.store_address_id
    inner join fixed_exposures fe
        on fe.customer_id = bi.customer_id
        and bi.p_creation_date >= fe.first_exposure_at
    where true
        and bi.banner_type = 'CollectionShoppableBanners'
        and bi.store_address_id is not null
    group by 1
)

,widget_impression as (-- we cannot filter for a given store, but this should be okkay
    select
        wi.customer_id,
        count(distinct wi.event_id) as widget_collection_impressions
    from delta.customer_behaviour_odp.enriched_custom_event__widget_impression_v3 wi
    inner join calendar_dates
        on wi.p_creation_date = calendar_dates.calendar_date
    inner join fixed_exposures fe
        on fe.customer_id = wi.customer_id
        and wi.p_creation_date >= fe.first_exposure_at
    where true
        and widget_app_location = 'Collection'
    group by 1
)

,collection_opened as (
    select
        co.customer_id,
        count(distinct co.event_id) as collection_opened,
        count(distinct concat(cast(co.dynamic_session_id as varchar), cast(co.collection_id as varchar))) as distinct_collection_opened_per_session
    from delta.customer_behaviour_odp.enriched_custom_event__collection_opened_v3 co
    inner join calendar_dates
        on co.p_creation_date = calendar_dates.calendar_date
    inner join stores
        on stores.store_address_id = co.store_address_id
    inner join fixed_exposures fe
        on fe.customer_id = co.customer_id
        and co.p_creation_date >= fe.first_exposure_at
    where true
    group by 1
)

,cvr as (
    select 
        sa.customer_id,
        count(distinct sa.dynamic_session_id) as store_accesss_session,
        count(distinct oc.dynamic_session_id) as order_created_session 
    from delta.customer_behaviour_odp.enriched_custom_event__store_accessed_v3 AS sa
    inner join calendar_dates cd
        on cd.calendar_date = sa.p_creation_date
    inner join stores
        on stores.store_address_id = sa.store_address_id
    inner join fixed_exposures fe
        on fe.customer_id = sa.customer_id
        and sa.p_creation_date >= fe.first_exposure_at
    left join delta.customer_behaviour_odp.enriched_custom_event__order_created_v3 oc
        on sa.dynamic_session_id = oc.dynamic_session_id
        and sa.store_address_id = oc.store_address_id
    where true
        and not (cardinality(promotion_ids) = 1 and contains(promotion_ids, -1)) --exclude the promotion_id -1 if appears alone
        and promotion_ids is not null
    group by 1
) 

select 
    fe.customer_id,
    fe.variant,
    fe.first_exposure_at,
    -- Metrics banners
    coalesce(bi.banner_impressions,0) as banner_impressions,
    coalesce(bi.distinct_banner_impressions_per_session,0) as distinct_banner_impressions_per_session,
    -- Metrics widget
    coalesce(wi.widget_collection_impressions,0) as widget_collection_impressions,
    -- Metrics collections
    coalesce(co.collection_opened,0) as collection_opened,
    coalesce(co.distinct_collection_opened_per_session,0) as distinct_collection_opened_per_session,
    -- Metrics cvr
    coalesce(cvr.store_accesss_session,0) as store_access_session,
    coalesce(cvr.order_created_session,0) as order_created_session
from fixed_exposures fe
left join banner_impression__shoppable bi
    on fe.customer_id = bi.customer_id
left join widget_impression wi
    on fe.customer_id = wi.customer_id
left join collection_opened co
    on fe.customer_id = co.customer_id
left join cvr 
    on fe.customer_id = cvr.customer_id
where true