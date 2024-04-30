--v1
with calendar_dates as (select
        calendar_date
    from unnest(sequence(date_add('day',-30,current_date), date_add('day',-1,current_date), interval '1' day)) as dates (calendar_date)
)

--pk store_id
,stores_info as (select
        store_id,
        case
            when store_subvertical2 in ('Food - Food', 'Food - Other') then 'Food'
            when store_subvertical2 in ('Groceries') then 'Groceries'
        end as store_bu
    from delta.partner_stores_odp.stores_v2
    where 1=1
        and p_end_date is null
        and store_subvertical2 in ('Food - Food', 'Food - Other', 'Groceries')
)

--pk concat dynamic_session_id + store_id + creation_time
,stores_accessed as (select distinct
        sa.dynamic_session_id,
        sa.store_id,
        case when sa.origin='DoubleGlovo' then 'DoubleGlovo' else 'NotDoubleGlovo' end as store_accessed_channel,
        sa.creation_time
    from delta.customer_behaviour_odp.enriched_custom_event__store_accessed_v3 sa
    where 1=1
        and sa.p_creation_date in (select calendar_date from calendar_dates)
        and dynamic_session_id is not null
        and store_id is not null
        and creation_time is not null
)

--pk order_id
,orders_created as (select distinct
        oc.order_id,
        oc.customer_id,
        od.store_id,
        element_at(array_sort(array_agg(if(oc.number_of_orders_groceries=0, true, false))),1) as user_is_GNC, --picks false before true
        element_at(array_sort(array_agg(oc.country)),1) as country,
        element_at(array_sort(array_agg(oc.city)),1) as city,
        element_at(array_sort(array_agg(oc.p_creation_date)),1) as p_creation_date,
        element_at(array_sort(array_agg(oc.creation_time)),1) as creation_time,
        element_at(array_sort(array_agg(oc.dynamic_session_id)),1) as dynamic_session_id,
        element_at(array_sort(array_agg(oc.user_is_prime)),1) as user_is_prime --picks false before true
    from delta.central_order_descriptors_odp.order_descriptors_v2 od
    inner join delta.customer_behaviour_odp.enriched_custom_event__order_created_v3 oc
        on oc.order_id = od.order_id
        and oc.customer_id is not null
        and oc.store_id is not null
        and oc.country is not null
        and oc.city is not null
        and oc.p_creation_date is not null
        and oc.creation_time is not null
        and oc.dynamic_session_id is not null
        and oc.user_is_prime is not null
    where 1=1
        and od.order_parent_relationship_type is null
        and od.order_final_status ='DeliveredStatus'
        and od.p_creation_date in (select calendar_date from calendar_dates)
    group by 1,2,3
)

--pk order_id
,orders_enriched as (select distinct
        oc.order_id,
        oc.customer_id,
        oc.store_id,
        oc.user_is_GNC,
        oc.country,
        oc.city,
        oc.p_creation_date,
        oc.creation_time,
        oc.dynamic_session_id,
        oc.user_is_prime,
        s.store_bu,
        element_at(array_sort(array_agg(sa.store_accessed_channel)),1) as store_accessed_channel-- picks DG before not DG
    from orders_created oc
    left join stores_info s -- one to one
        on oc.store_id = s.store_id
    left join stores_accessed sa -- one to many
        on oc.store_id = sa.store_id
        and oc.dynamic_session_id = sa.dynamic_session_id
        and oc.creation_time > sa.creation_time
    where 1=1
        and s.store_id is not null
        and store_accessed_channel is not null
    group by 1,2,3,4,5,6,7,8,9,10,11
)

select distinct
    g.p_creation_date,
    g.dynamic_session_id,
    case when g.dynamic_session_id = f.dynamic_session_id then 1 else 0 end as is_same_session,
    g.order_id as grocery_order_id,
    f.order_id as food_order_id,
    g.user_is_GNC,
    g.user_is_prime,
    g.city,
    g.store_accessed_channel,
    date_diff('second', f.creation_time,g.creation_time) as delta_seconds,
    case 
        when date_diff('second', f.creation_time,g.creation_time)>0 AND date_diff('second', f.creation_time,g.creation_time)<=3600 then '(0h-1h]'
        when date_diff('second', f.creation_time,g.creation_time)>3600 AND date_diff('second', f.creation_time,g.creation_time)<=14400 then '(1h-4h]'
        when date_diff('second', f.creation_time,g.creation_time)>14400 then '(4h-24h)'
    end as bucket_delta_seconds,
    case when ad.campaign_id='DoubleGlovo' then 1 else 0 end as is_ad_shown
from orders_enriched as g
left join orders_enriched as f
    on g.customer_id = f.customer_id
    and g.creation_time >= f.creation_time
    and g.store_bu = 'Groceries'
    and f.store_bu = 'Food'
    and date_diff('second', f.creation_time,g.creation_time) <= 86400 -- seconds in 24h
    and g.p_creation_date >= f.p_creation_date
left join delta.customer_behaviour_odp.enriched_custom_event__ad_container_impression_v3 ad
    on f.order_id = ad.order_id
    and ad.campaign_id = 'DoubleGlovo'
    and date(ad.creation_time) in (select calendar_date from calendar_dates)
where 1=1
    and g.store_bu = 'Groceries'
    and f.order_id is not null
--endv1


