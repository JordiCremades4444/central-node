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

--pk customer_id
,glovo_customers as (
    select
        u.user_id as customer_id
    from delta.central_users_odp.users_v2 u
    where true
        and not user_is_staff
        and not user_is_glovo_employee
        and user_type = 'Customer'
)

--pk store_address_id
,stores_segmented as (
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
    inner join delta.mfc_inventory_odp.products_v2 p
        on p.store_address_id = bp.store_address_id
        and p.product_sku = bp.product_external_id
        and p.product_category_level_one in ('Produce', 'Ready To Consume', 'Meat / Seafood', 'Bread / Bakery', 'Dairy / Chilled / Eggs')
)

--pk order_id
,subsequent_orders as (
    select 
        od.order_id,
        od.p_creation_date,
        od.customer_id,
        ss.segment_2,
        ss.country_code,
        case when fo.order_id is not null then true else false end as is_fresh_order
    from delta.central_order_descriptors_odp.order_descriptors_v2 as od
    inner join calendar_dates_retention cd
        on cd.calendar_date = od.p_creation_date
    inner join migrated_stores ms
        on ms.store_address_id = od.store_address_id
    inner join stores_segmented ss
        on ss.store_address_id = od.store_address_id
    left join fresh_orders fo
        on fo.order_id = od.order_id
    inner join glovo_customers gc
        on gc.customer_id = od.customer_id
    where true
        and od.order_final_status = 'DeliveredStatus'
        and od.order_parent_relationship_type is null
)

,outliers_count as (
    select
        country_code,
        segment_2,
        customer_id,
        count(distinct order_id) as distinct_orders
    from subsequent_orders
    group by 1,2,3
)

,percentile_ranks as (
    select
        country_code,
        segment_2,
        customer_id,
        distinct_orders,
        percent_rank() over (partition by country_code, segment_2 order by distinct_orders) as order_percentile
    from outliers_count
)

,not_outlier_customers as (
    select 
        country_code,
        segment_2,
        customer_id
    from percentile_ranks
    where true 
        and order_percentile < {outliers_threshold}
)

--pk order_id
,retention_subsequent_o as (select distinct
        o.p_creation_date,
        o.order_id,
        coalesce(count(distinct o_next.order_id),0) as orders_28d,
        coalesce(count(distinct case when o_next.is_fresh_order then o_next.order_id else null end),0) as orders_28d_f
    from subsequent_orders as o
    inner join calendar_dates cd
        on o.p_creation_date = cd.calendar_date
    left join subsequent_orders as o_next
        on o.customer_id = o_next.customer_id
        and o.order_id != o_next.order_id
        and o_next.p_creation_date >= o.p_creation_date and o_next.p_creation_date <= date_add('day', 28, o.p_creation_date)
        and o.segment_2 = o_next.segment_2
    group by 1,2
)

--raw table is at an order level
,metrics_subsequent_orders as (
    select
        o.country_code as country,
        o.segment_2,
        --subsequent orders point 0 (so the order we are lookinga at)
        count(distinct case when o.is_fresh_order then o.order_id else null end) as f_subsequent_orders_0,
        count(distinct case when not o.is_fresh_order then o.order_id else null end) as nf_subsequent_orders_0,
        --avg subsequent orders all
        avg(case when o.is_fresh_order then rs.orders_28d else null end) as f_avg_subsequent__all_orders,
        avg(case when not o.is_fresh_order then rs.orders_28d else null end) as nf_avg_subsequent_all_orders,
        --avg subsequent f ordres
        avg(case when o.is_fresh_order then rs.orders_28d_f else null end) as f_avg_subsequent__f_orders,
        avg(case when not o.is_fresh_order then rs.orders_28d_f else null end) as nf_avg_subsequent_f_orders,
        --feedback impact in fresh orders
        avg(case when o.is_fresh_order and f.order_id is not null then rs.orders_28d else null end) ff_avg_subsequent_all_orders,
        avg(case when o.is_fresh_order and f.order_id is not null then rs.orders_28d_f else null end) ff_avg_subsequent_f_orders,
        -- checks
        count(*) as n_rows,
        count(distinct o.order_id) as n_distinct_order_id
    from subsequent_orders o
    inner join calendar_dates cd
        on cd.calendar_date = o.p_creation_date
    inner join not_outlier_customers noc -- we exclude heavy users, they have too much weight in this metric
        on o.segment_2 = noc.segment_2
        and o.country_code = noc.country_code
        and o.customer_id = noc.customer_id
    left join retention_subsequent_o rs
        on o.order_id = rs.order_id
        and o.p_creation_date = rs.p_creation_date
    left join feedbacks f
        on f.order_id = o.order_id
    where true
    group by 1,2
)

select * from metrics_subsequent_orders
