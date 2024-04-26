--v1
with calendar_dates as (select
        calendar_date
    from unnest(sequence(date_add('day',-30,current_date), date_add('day',-1,current_Date), interval '1' day)) as dates (calendar_date)
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

select 
    count(distinct f.order_id) as n_food_orders,
    1.00000*(count(distinct f.order_id) filter (where g.order_id is not null))/count(distinct f.order_id) n_groceries_same_session,
    1.00000*(count(distinct f.order_id) filter (where g.order_id is not null and g.store_accessed_channel = 'DoubleGlovo'))/count(distinct f.order_id) n_groceries_same_session_with_dg,
    1.00000*(count(distinct f.order_id) filter (where g.order_id is not null and g.store_accessed_channel = 'NotDoubleGlovo'))/count(distinct f.order_id) n_groceries_same_session_with_no_dg,
    1.00000*(count(distinct f.order_id) filter (where g2.order_id is not null))/count(distinct f.order_id) n_groceries_24h,
    1.00000*(count(distinct f.order_id) filter (where g2.order_id is not null and g2.store_accessed_channel = 'DoubleGlovo'))/count(distinct f.order_id) n_groceries_24h_with_dg,
    1.00000*(count(distinct f.order_id) filter (where g2.order_id is not null and g2.store_accessed_channel = 'NotDoubleGlovo'))/count(distinct f.order_id) n_groceries_24h_with_no_dg
from orders_enriched f
left join orders_enriched g
    on f.creation_time <= g.creation_time
    and f.customer_id = g.customer_id
    and f.dynamic_session_id = g.dynamic_session_id
    --and f.p_creation_date <= g.p_creation_date
    and g.store_bu = 'Groceries'
left join orders_enriched g2
    on f.creation_time <= g2.creation_time
    and f.customer_id = g2.customer_id
    and date_diff('second', f.creation_time,g2.creation_time) <= 86400 -- seconds in 24h
    --and f.p_creation_date <= g2.p_creation_date
    and g2.store_bu = 'Groceries'
where 1=1
    and f.store_bu = 'Food'
--endv1