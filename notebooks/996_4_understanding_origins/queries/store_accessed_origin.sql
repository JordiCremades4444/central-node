with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date_add('day',-{start_previous_to_current_date},current_date),-- initial date
        date_add('day',-{end_previous_to_current_date},current_date),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

, qcommerce_stores as (
    select distinct
        store_id
    from delta.partner_stores_odp.stores_v2 
    where 1=1 
        and store_vertical = 'QCommerce'
        and store_is_enabled
        and not store_is_deleted
        and p_end_date is null 
)

,stores_accessed as (
    select 
        origin,
        case when qcs.store_id is not null then true else false end as is_qc_store
    from delta.customer_behaviour_odp.enriched_custom_event__store_accessed_v3 sa
    left join qcommerce_stores qcs
        on sa.store_id = qcs.store_id
    where 1=1
        and sa.p_creation_date in (select calendar_date from calendar_dates)
)

select 
    origin,
    count_if(is_qc_store) as events_qc,
    count_if(not is_qc_store) as events_not_qc
from stores_accessed
group by 1 
order by 1 asc
