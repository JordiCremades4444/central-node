with calendar_dates as (
    select 
        calendar_date 
    from unnest(sequence(date({start_date}), date({end_date}), interval '1' day)) as cte (calendar_date)
    where true
)

,calendar_dates_retention as (
    select 
        calendar_date 
    from unnest(sequence(date({start_date}), date({end_date_retention}), interval '1' day)) as cte (calendar_date)
    where true
)

-- pk customer_id
,glovo_customers as (
    select
        u.user_id as customer_id
    from delta.central_users_odp.users_v2 u
    where true
        and not user_is_staff
        and not user_is_glovo_employee
        and user_type = 'Customer'
)

--pk pair country_code, store_name
,top_brands_last_snapshot as (
    select distinct
        tp.country_code,
        tp.store_name
    from delta.mfc__groceries_content_availability_targets__odp.groceries_top_partners tp
    where true
        and tp.p_ingestion_date = (select max(p_ingestion_date) from delta.mfc__groceries_content_availability_targets__odp.groceries_top_partners) 
        and country_code is not null
        and store_name is not null
)

--pk store_address_id
,migrated_stores as (
    select distinct
        store_address_id
    from delta.mfc_inventory_odp.products_v2 p
)

--pk store_address_id
,stores as (
    select distinct
        c.country_code, 
        s.store_subvertical,
        s.store_sub_business_unit,        
        s.store_name,
        sa.store_id,
        sa.store_address_id,
        case when t.store_name is not null then true else false end as is_top_partner,
        case when ms.store_address_id is not null then true else false end as is_migrated
    from delta.partner_stores_odp.store_addresses_v2 sa
    left join delta.partner_stores_odp.stores_v2 s
        on sa.store_id = s.store_id
    left join delta.central_geography_odp.cities_v2 c
        on s.city_code = c.city_code
    left join top_brands_last_snapshot t 
        on t.store_name = s.store_name
        and t.country_code = c.country_code
    inner join migrated_stores ms -- we only keep migrated universe
        on ms.store_address_id = sa.store_address_id
    where true
        and sa.p_end_date is null
        and s.p_end_date is null
        and s.store_vertical = 'QCommerce'
        and s.store_subvertical in ('QCPartners', 'MFC')
        and s.store_subvertical3 = 'Groceries'
        and s.store_is_enabled
        and not (s.store_is_deleted or s.store_is_deleted is null)
        and not (sa.store_address_is_deleted or sa.store_address_is_deleted is null)
        and c.country_code not in ('AR','BO','BY','CL','CO','CR','DO','EC','EG','GT','PE','UY','ZA','TR','PR','BR','HN','PA','FR')
)

--pk store_address_id
,segment_2 as (
    select 
        country_code,
        store_id,
        store_address_id,
        store_name,
        case 
            when s.store_subvertical = 'MFC' then 'MFC'
            when s.store_subvertical = 'QCPartners' and upper(s.store_sub_business_unit) not in ('CONVENIENCE','SUPERMARKET','OTHER') then 'Specialties'
            when s.store_subvertical = 'QCPartners' and upper(s.store_sub_business_unit) in ('CONVENIENCE','SUPERMARKET','OTHER') then 'Groceries Partner'
        else 'undefined' end as segment_2,
        is_migrated
    from stores s
)

--pk store_address_id
,stores_segmented as (
    select
        *
    from segment_2 
)

--pk order_id
,fresh_orders as (
    select
        distinct bp.order_id
    from delta.customer_bought_products_odp.bought_products_v2 bp
    inner join calendar_dates_retention cd -- to later compute retention
        on bp.p_creation_date = cd.calendar_date
    inner join migrated_stores ms
        on ms.store_address_id = bp.store_address_id
    inner join stores_segmented ss
        on ss.store_address_id = bp.store_address_id
    inner join delta.central_order_descriptors_odp.order_descriptors_v2 od
        on od.order_id = bp.order_id
        and od.order_parent_relationship_type is null
    inner join delta.mfc_inventory_odp.products_v2 p
        on p.store_address_id = bp.store_address_id
        and p.product_sku = bp.product_external_id
        and p.product_category_level_one in ('Produce', 'Ready To Consume', 'Meat / Seafood', 'Bread / Bakery', 'Dairy / Chilled / Eggs')
)

--pk order_id
,feedbacks as (
    select distinct
        order_id
    from delta.central_ratings_odp.ratings r
    inner join calendar_dates cd
        on r.p_creation_date = cd.calendar_date
    where true
        and partner_rating_evaluation = 'NEGATIVE'
        and contains(r.partner_rating_reasons, 'NOT_FRESH')
)

--pk order_id
,subsequent_orders as (
    select 
        od.order_id,
        od.p_creation_date,
        od.customer_id,
        od.order_created_at,
        ss.segment_2,
        ss.country_code,
        od.order_final_status,
        od.order_parent_relationship_type,
        case when fo.order_id is not null then true else false end as is_fresh_order
    from delta.central_order_descriptors_odp.order_descriptors_v2 as od
    inner join calendar_dates_retention cd
        on cd.calendar_date = od.p_creation_date
    inner join migrated_stores ms
        on ms.store_address_id = od.store_address_id
    inner join stores_segmented ss
        on ss.store_address_id = od.store_address_id
    inner join glovo_customers as gc
        on gc.customer_id = od.customer_id
    left join fresh_orders fo
        on fo.order_id = od.order_id
    where true
        and od.order_final_status = 'DeliveredStatus'
        and od.order_parent_relationship_type is null
)

--pk order_id
,f_retention_subsequent_o as (select distinct
        o.p_creation_date,
        o.order_id,
        count(distinct o_next.order_id) as orders_28d
    from subsequent_orders as o
    inner join calendar_dates cd
        on o.p_creation_date = cd.calendar_date
    left join subsequent_orders as o_next
        on o.customer_id = o_next.customer_id
        and o.order_id != o_next.order_id
        and o_next.order_created_at > o.order_created_at
        and o_next.order_created_at <= date_add('day', 28, o.order_created_at)
        and o.segment_2 = o_next.segment_2
    group by 1,2
)

--raw table is at an order level
,metrics_subsequent_orders as (
    select
        country_code as country,
        segment_2,
        -- subsequent volumes
        avg(case when o.is_fresh_order then frs.orders_28d else null end) as f_subsequent_orders,
        avg(case when (not o.is_fresh_order) then frs.orders_28d else null end) as nf_subsequent_orders,
        -- feedbacks
        avg(case when o.is_fresh_order and f.order_id is not null then frs.orders_28d else null end) as f_feedback_subsequent_orders,
        avg(case when (not o.is_fresh_order) and f.order_id is not null then frs.orders_28d else null end) as nf_feedback_subsequent_orders,
        -- no zeros
        count(distinct case when o.is_fresh_order and f.order_id is not null and frs.orders_28d > 0 then o.order_id else null end) as n_f_feedback_subsequent_orders_no0,
        avg(case when o.is_fresh_order and (f.order_id is not null) and frs.orders_28d > 0 then frs.orders_28d else null end) as f_feedback_subsequent_orders_no0,
        count(distinct case when (not o.is_fresh_order) and f.order_id is not null and frs.orders_28d > 0 then o.order_id else null end) as n_nf_feedback_subsequent_orders_no0,
        avg(case when (not o.is_fresh_order) and f.order_id is not null and frs.orders_28d > 0 then frs.orders_28d else null end) as nf_feedback_subsequent_orders_no0,
        -- checks
        count(*) as n_rows,
        count(distinct o.order_id) as n_distinct_order_id
    from subsequent_orders o
    inner join calendar_dates cd
        on cd.calendar_date = o.p_creation_date
    left join f_retention_subsequent_o frs
        on o.order_id = frs.order_id
    left join feedbacks f
        on f.order_id = o.order_id
    group by 1,2
)

select * from metrics_subsequent_orders
