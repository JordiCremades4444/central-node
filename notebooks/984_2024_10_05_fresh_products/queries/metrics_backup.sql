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

--pk store_address_id
,segment_1 as (
    select 
        country_code,
        store_id,
        store_address_id,
        case 
            when s.store_subvertical = 'MFC' then 'MFC'
            when s.store_subvertical = 'QCPartners' and upper(s.store_sub_business_unit) not in ('CONVENIENCE','SUPERMARKET','OTHER') then 'Specialties'
            when s.store_subvertical = 'QCPartners' and upper(s.store_sub_business_unit) in ('CONVENIENCE','SUPERMARKET','OTHER') and is_top_partner then 'Top Partner'
            when s.store_subvertical = 'QCPartners' and upper(s.store_sub_business_unit) in ('CONVENIENCE','SUPERMARKET','OTHER') and not is_top_partner then 'Non Top Partner'
        else 'undefined' end as segment_1,
        is_migrated
    from stores s
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
        segment_1.*,
        segment_2.segment_2
    from segment_1 
    left join segment_2
        on segment_1.store_address_id = segment_2.store_address_id
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
,f_retention as (select
        p_creation_date,
        o.order_id,
        if(customer_id is not null and store_name is not null,lead(p_creation_date) ignore nulls over(partition by o.customer_id,o.store_name order by o.order_created_at asc),null) as store_name_ret1,
        lead(p_creation_date) ignore nulls over(partition by o.customer_id,o.order_subvertical3 order by o.order_created_at asc) as order_subvertical3_ret1
    from delta.central_order_descriptors_odp.order_descriptors_v2 as o
    inner join calendar_dates_retention cd
        on cd.calendar_date = o.p_creation_date -- to compute retention
    inner join migrated_stores ms
        on ms.store_address_id = o.store_address_id
    inner join fresh_orders fo
        on fo.order_id = o.order_id 
    where 1=1
        and o.order_final_status = 'DeliveredStatus'
        and o.order_parent_relationship_type is null
)

--pk order_id
,f_retention_enriched as (select 
        order_id,
        if(date_diff('day',p_creation_date,store_name_ret1)<=28,true,false) as store_name_is_ret1,
        if(date_diff('day',p_creation_date,order_subvertical3_ret1)<=28,true,false) as order_subvertical3_is_ret1
    from f_retention
    where 1=1
    order by p_creation_date desc
)

--the raw table before aggregations is at a bought_product_id level
,metrics as (
    select
        ss.country_code as country,
        ss.segment_1,
        ss.segment_2,
        --customers
        count(distinct od.customer_id) as all_customers,
        count(distinct case when fo.order_id is not null then od.customer_id else null end) as f_customers,
        count(distinct case when fo.order_id is null then od.customer_id else null end) as nf_customers,
        --fresh orders
        count(distinct od.order_id) as all_orders,
        count(distinct case when fo.order_id is not null then od.order_id else null end) as f_orders,
        count(distinct case when fo.order_id is null then od.order_id else null end) as nf_orders,
        --gmv
        sum(bp.products_value_eur) as all_gmv,
        sum(case when fo.order_id is not null then bp.products_value_eur else 0 end) as fo_gmv,
        sum(case when fo.order_id is null then bp.products_value_eur else 0 end) as nfo_gmv,
        --uipo
        count(distinct bp.bought_product_id) as n_uipo_all_orders,
        count(distinct case when fo.order_id is not null then bp.bought_product_id else null end) as n_uipo_fresh_orders,
        count(distinct case when fo.order_id is null then bp.bought_product_id else null end) as n_uipo_not_fresh_orders,
        --pna
        count(distinct case when poi.order_is_pna then bp.order_id else null end) as pna_all_orders,
        count(distinct case when poi.order_is_pna and fo.order_id is not null then bp.order_id else null end) as f_orders_with_pna,
        count(distinct case when poi.order_is_pna and fo.order_id is null then bp.order_id else null end) as nf_orders_with_pna,
        --feedbacks
        count(distinct case when f.order_id is not null then bp.order_id else null end) as ratings_all_orders,
        count(distinct case when f.order_id is not null and fo.order_id is not null then bp.order_id else null end) as ratings_f_orders,
        count(distinct case when f.order_id is not null and fo.order_id is null then bp.order_id else null end) as ratings_nf_orders,
        --retention Groceries
        count(distinct case when roi.order_subvertical3_is_ret1 then bp.order_id else null end) as all_orders_retained_Groceries,
        count(distinct case when roi.order_subvertical3_is_ret1 and fo.order_id is not null then bp.order_id else null end) as f_orders_retained_Groceries,
        count(distinct case when roi.order_subvertical3_is_ret1 and fo.order_id is null then bp.order_id else null end) as nf_orders_retained_Groceries,
        --retention store_id
        -- count(distinct case when roi.store_name_is_ret1 then bp.order_id else null end) as all_orders_retained_Store,
        -- count(distinct case when roi.store_name_is_ret1 and fo.order_id is not null then bp.order_id else null end) as f_orders_retained_Store,
        -- count(distinct case when roi.store_name_is_ret1 and fo.order_id is null then bp.order_id else null end) as nf_orders_retained_Store,
        --retention Groceries feedbacks
        count(distinct case when roi.order_subvertical3_is_ret1 and f.order_id is not null then bp.order_id else null end) as all_feedback_orders_retained_Groceries,
        count(distinct case when roi.order_subvertical3_is_ret1 and f.order_id is not null and fo.order_id is not null then bp.order_id else null end) as f_feedback_orders_retained_Groceries,
        count(distinct case when roi.order_subvertical3_is_ret1 and f.order_id is not null and fo.order_id is null then bp.order_id else null end) as nf_feedback_orders_retained_Groceries,
        --retention store_id feedbacks
        -- count(distinct case when roi.store_name_is_ret1 and f.order_id is not null then bp.order_id else null end) as all_feedback_orders_retained_Store,
        -- count(distinct case when roi.store_name_is_ret1 and f.order_id is not null and fo.order_id is not null then bp.order_id else null end) as f_feedback_orders_retained_Store,
        -- count(distinct case when roi.store_name_is_ret1 and f.order_id is not null and fo.order_id is null then bp.order_id else null end) as nf_feedback_orders_retained_Store,
        --only fresh retention Groceries
        -- count(distinct case when fre.order_subvertical3_is_ret1 then bp.order_id else null end) as all_orders_fretained_Groceries,
        count(distinct case when fre.order_subvertical3_is_ret1 and fo.order_id is not null then bp.order_id else null end) as f_orders_fretained_Groceries,
        -- count(distinct case when fre.order_subvertical3_is_ret1 and fo.order_id is null then bp.order_id else null end) as nf_orders_fretained_Groceries,
        --only fresh retention Groceries feedbacks
        -- count(distinct case when fre.order_subvertical3_is_ret1 and f.order_id is not null then bp.order_id else null end) as all_feedback_orders_fretained_Groceries,
        count(distinct case when fre.order_subvertical3_is_ret1 and f.order_id is not null and fo.order_id is not null then bp.order_id else null end) as f_feedback_orders_fretained_Groceries,
        -- count(distinct case when fre.order_subvertical3_is_ret1 and f.order_id is not null and fo.order_id is null then bp.order_id else null end) as nf_feedback_orders_fretained_Groceries,
        --only fresh retention store_id feedbacks
        -- count(distinct case when fre.store_name_is_ret1 and f.order_id is not null then bp.order_id else null end) as all_feedback_orders_fretained_Store,
        -- count(distinct case when fre.store_name_is_ret1 and f.order_id is not null and fo.order_id is not null then bp.order_id else null end) as f_feedback_orders_fretained_Store,
        -- count(distinct case when fre.store_name_is_ret1 and f.order_id is not null and fo.order_id is null then bp.order_id else null end) as nf_feedback_orders_fretained_Store,
        --check
        count(*) as n_rows,
        count(distinct bp.bought_product_id) as n_distinct_bought_product_ids
    from delta.central__bought_products_looker__odp.bought_products bp
    inner join calendar_dates cd
        on cd.calendar_date = bp.p_creation_date
    inner join migrated_stores ms
        on ms.store_address_id = bp.store_address_id
    inner join stores_segmented ss
        on ss.store_address_id = bp.store_address_id
    inner join delta.central_order_descriptors_odp.order_descriptors_v2 od
        on od.order_id = bp.order_id
    inner join delta.mfc__pna__odp.pna_orders_info poi
        on poi.order_id = bp.order_id
    inner join delta.central__retention_orders__odp.retention_order_info roi
        on roi.order_id = bp.order_id
    left join fresh_orders fo
        on fo.order_id = bp.order_id 
    left join feedbacks f
        on f.order_id = bp.order_id
    left join f_retention_enriched fre
        on fre.order_id = bp.order_id
    where true
        and od.order_parent_relationship_type is null
        and od.order_final_status = 'DeliveredStatus'
    group by 1,2,3
)

select * from metrics