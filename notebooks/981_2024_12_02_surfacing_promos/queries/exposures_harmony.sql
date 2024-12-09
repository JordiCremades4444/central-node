with calendar_dates as (
    select
        calendar_date
    from unnest(sequence(
        date('{end_date}') - interval '{num_days}' day, 
        date('{end_date}'), 
        interval '1' day
    )) as cte (calendar_date)
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
        and (contains(promotion_types,'PERCENTAGE_DISCOUNT') or contains(promotion_types,'TWO_FOR_ONE') or contains(promotion_types,'FLAT_PRODUCT'))
)

select 
    customer_id,
    variant,
    min(event_date) as first_exposure_at
from groceries_store_accessed_surfacing_promos 
where true
    and session_rn = 1 --de cada session em quedo amb la primera entrada
group by 1,2


