--v1
with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date_add('day',-365,current_date),-- initial date
        date_add('day',-1,current_date),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
        and calendar_date >= date('2023-06-10')
)

,stores_accessed as (select distinct
        sa.dynamic_session_id,
        sa.store_id,
        case when sa.origin='DoubleGlovo' then 'DoubleGlovo' else 'NotDoubleGlovo' end store_accessed_channel,
        sa.creation_time as store_accessed_time -- for each session we keep the minimum DG touchpoint
    from delta.customer_behaviour_odp.enriched_custom_event__store_accessed_v3 sa
    where 1=1
        and sa.p_creation_date in (select calendar_date from calendar_dates)
)

,orders_filtered as (select distinct
        od.order_id,
        od.p_creation_date,
        od.customer_id,
        od.order_total_purchase_eur,
        od.order_subvertical2,
        -- aov
        avg(if(od.order_subvertical2 = 'Groceries',od.order_total_purchase_eur,null)) over (partition by od.customer_id order by od.p_creation_date asc range between interval '31' day preceding and interval '1' day preceding) as aov_groceries_preceding30d,
        avg(if(od.order_subvertical2 = 'Groceries',od.order_total_purchase_eur,null)) over (partition by od.customer_id order by od.p_creation_date asc range between interval '1' day following and interval '31' day following) as aov_groceries_next30d,
        -- gmv
        sum(if(od.order_subvertical2 = 'Groceries',od.order_total_purchase_eur,0.0)) over (partition by od.customer_id order by od.p_creation_date asc range between interval '31' day preceding and interval '1' day preceding) as gmv_groceries_preceding30d,
        sum(if(od.order_subvertical2 = 'Groceries',od.order_total_purchase_eur,0.0)) over (partition by od.customer_id order by od.p_creation_date asc range between interval '1' day following and interval '31' day following) as gmv_groceries_next30d,
        -- number of orders
        count(if(od.order_subvertical2 = 'Groceries',order_id,null)) over (partition by od.customer_id order by od.p_creation_date asc range between interval '31' day preceding and interval '1' day preceding) as n_groceries_orders_preceding30d,
        count(if(od.order_subvertical2 = 'Groceries',order_id,null)) over (partition by od.customer_id order by od.p_creation_date asc range between interval '1' day following and interval '31' day following) as n_groceries_orders_next30d,
        count(if(od.order_subvertical2 = 'Groceries',order_id,null)) over (partition by od.customer_id order by od.p_creation_date asc range between interval '1' day following and interval '29' day following) as n_groceries_orders_next28d
    from delta.central_order_descriptors_odp.order_descriptors_v2  as od
    where 1=1
        and order_parent_relationship_type IS NULL 
        and order_final_status = 'DeliveredStatus'
        and p_creation_date in (select calendar_date from calendar_dates)  
)

,orders_created_enriched as (select distinct
        oc.p_creation_date as p_creation_date,
        
        -- order info
        oc.order_id as order_id,
        oc.country as country,
        oc.store_id as store_id,
        o.order_subvertical2 as order_subvertical2,
        sa.store_accessed_channel as store_accessed_channel,
        
        -- customer info
        oc.customer_id as customer_id,
        oc.user_is_prime as user_is_prime,
        if(oc.number_of_orders_groceries=0, true, false) as user_is_GNC,
        date_diff('day', date(user_created_at), oc.p_creation_date) / 28 as months_old,
        
        -- preceding and following calculations
        coalesce(round(o.aov_groceries_preceding30d,2),0) as aov_groceries_preceding30d,
        coalesce(round(o.aov_groceries_next30d,2),0) as aov_groceries_next30d,
        coalesce(round(o.gmv_groceries_preceding30d,2), 0) AS gmv_groceries_preceding30d,
        coalesce(round(o.gmv_groceries_next30d,2), 0) AS gmv_groceries_next30d,
        coalesce(o.n_groceries_orders_preceding30d, 0) AS n_groceries_orders_preceding30d,
        coalesce(o.n_groceries_orders_next30d, 0) AS n_groceries_orders_next30d,
        coalesce(o.n_groceries_orders_next28d,0) AS n_groceries_orders_next28d
    from delta.customer_behaviour_odp.enriched_custom_event__order_created_v3 oc
    inner join orders_filtered o
        on oc.order_id = o.order_id
        and oc.p_creation_date = o.p_creation_date
        and o.order_subvertical2='Groceries'
    left join stores_accessed sa
        on oc.dynamic_session_id = sa.dynamic_session_id
        and oc.store_id = sa.store_id
        and oc.creation_time > sa.store_accessed_time -- order creation after store access
    left join delta.central_users_odp.users_v2 u
        on u.user_id = oc.customer_id
    where 1=1
        and oc.p_creation_date in (select calendar_date from calendar_dates)
        and oc.p_creation_date >= date('2023-07-13') -- to give enough room for completing the preceding 30days.
        and oc.p_creation_date < date('2023-11-10') -- 10th to give enough room for completing the following 30days. Today is 12/12/2023
        and country in ('AM', 'BA', 'CI', 'ES', 'GE', 'GH', 'HR', 'IT', 'KE', 'KG', 'KZ', 'MA', 'ME', 'NG', 'PL', 'PT', 'RO', 'RS', 'SI', 'UA', 'UG')
        --and order_subvertical2 = 'Groceries'
)

select * from orders_created_enriched
--endv1