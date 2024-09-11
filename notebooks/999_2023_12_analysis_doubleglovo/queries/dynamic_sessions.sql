--v1
with calendar_dates as (select
        calendar_date
    from unnest(sequence(date_add('day',-30,current_date), date_add('day',-1,current_Date), interval '1' day)) as dates (calendar_date)
)

-- pk store_id + dynamic_session_id
,stores_accessed as (select
        sa.dynamic_session_id,
        sa.store_id,
        sa.customer_id,
        sa.creation_time,
        element_at(array_sort(array_agg(case when sa.origin='DoubleGlovo' then 'DoubleGlovo' else 'NotDoubleGlovo' end)),1) store_accessed_channel
    from delta.customer_behaviour_odp.enriched_custom_event__store_accessed_v3 sa
    where 1=1
        and sa.p_creation_date in (select calendar_date from calendar_dates)
        and dynamic_session_id is not null
        and store_id is not null
        and creation_time is not null
    group by 1,2,3,4
)

,dynamic_session_ids as (select distinct
        dynamic_session_id,
        creation_time,
        campaign_id,
        customer_id
    from delta.customer_behaviour_odp.enriched_custom_event__ad_container_impression_v3 ad
    where 1=1
        and ad.campaign_id = 'DoubleGlovo'
        and ad.p_creation_date in (select calendar_date from calendar_dates)
)

,dynamic_session_ids_ranked as (select 
        *,
        dense_rank() over(partition by dynamic_session_id order by creation_time asc) as rank
    from dynamic_session_ids
)

-- pk dynamic_session_id
,first_ad as (select 
        *
    from dynamic_session_ids_ranked
    where 1=1
        and rank = 1
)

,first_ad_paired_stores_accessed as (select 
        fa.dynamic_session_id,
        fa.campaign_id,
        fa.customer_id,
        sa.dynamic_session_id as store_accessed_dynamic_session_id,
        sa.store_id,
        sa.store_accessed_channel,
        date_diff('second', fa.creation_time,sa.creation_time) as delta_time,
        case when sa.dynamic_session_id is not null then
            (case when fa.dynamic_session_id = sa.dynamic_session_id then 1 else 0 end)
        end as is_same_session,
        rank() over(partition by fa.dynamic_session_id order by date_diff('second', fa.creation_time,sa.creation_time) asc) as rank
    from first_ad fa
    left join stores_accessed sa
        on fa.customer_id = sa.customer_id
        and fa.creation_time <= sa.creation_time
        and date_diff('second', fa.creation_time,sa.creation_time) <= 86400 -- seconds in 24h
)

select 
    dynamic_session_id,
    campaign_id,
    customer_id,
    store_id,
    delta_time,
    is_same_session
from first_ad_paired_stores_accessed
where 1=1
    and rank=1
--endv1