with calendar_dates as (
    select 
        calendar_date 
    from unnest(sequence(date({start_date}), date({end_date}), interval '1' day)) as cte (calendar_date)
    where true
)

,top_brands_last_snapshot as (
    select distinct
        tp.country_code,
        tp.store_name
    from delta.mfc__groceries_content_availability_targets__odp.groceries_top_partners tp
    where true
        and tp.p_ingestion_date = (select max(p_ingestion_date) from delta.mfc__groceries_content_availability_targets__odp.groceries_top_partners) 
)

,stores as (
    select 
        c.country_code, 
        case 
            when s.store_subvertical = 'MFC' then 'MFC'
            when s.store_subvertical = 'QCPartners' and upper(s.store_sub_business_unit) not in ('CONVENIENCE','SUPERMARKET','OTHER') then 'Specialties'
            when s.store_subvertical = 'QCPartners' and upper(s.store_sub_business_unit) in ('CONVENIENCE','SUPERMARKET','OTHER') and t.store_name is not null then 'Top Partner'
            when s.store_subvertical = 'QCPartners' and upper(s.store_sub_business_unit) in ('CONVENIENCE','SUPERMARKET','OTHER') and t.store_name is null then 'Non Top Partner'
        else 'undefined' end as segment,
        s.store_name,
        sa.store_address_id,
        sa.store_id
    from delta.partner_stores_odp.store_addresses_v2 sa
    left join delta.partner_stores_odp.stores_v2 s
        on sa.store_id = s.store_id
    left join delta.central_geography_odp.cities_v2 c
        on s.city_code = c.city_code
    left join top_brands_last_snapshot t 
        on t.store_name = s.store_name
        and t.country_code = c.country_code
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

,migrated_stores as (
    select distinct
        s.store_address_id,
        s.store_id
    from stores s
    inner join delta.mfc_inventory_odp.products_v2 p
        on s.store_address_id = p.store_address_id
)

,fresh_orders as (
    select
        distinct bp.order_id
    from delta.customer_bought_products_odp.bought_products_v2 bp
    inner join calendar_dates cd
        on bp.p_creation_date = cd.calendar_date
    inner join migrated_stores ms
        on ms.store_address_id = bp.store_address_id
    inner join delta.mfc_inventory_odp.products_v2 p
        on p.store_address_id = bp.store_address_id
        and p.product_sku = bp.product_external_id
        and p.product_category_level_one in ('Produce', 'Ready to Consume', 'Meat / Seafood')
)

,non_fresh_orders as (
    select
        distinct bp.order_id
    from delta.customer_bought_products_odp.bought_products_v2 bp
    inner join calendar_dates cd
        on bp.p_creation_date = cd.calendar_date
    inner join migrated_stores ms
        on ms.store_address_id = bp.store_address_id
    where true
        and bp.order_id not in (select order_id from fresh_orders)
)

,orders as (
    select
        od.order_country_code as country,
        s.segment,
        count(distinct case when fo.order_id is not null then bp.order_id else null end) as f_orders,
        count(distinct case when nfo.order_id is not null then bp.order_id else null end) as nf_orders,
        sum(case when fo.order_id is not null then bp.products_value_eur else null end) as f_value,
        sum(case when nfo.order_id is not null then bp.products_value_eur else null end) as nf_value,
        count(distinct case when fo.order_id is not null and roi.order_subvertical3_is_ret1 then bp.order_id else null end) as f_orders_ret,
        count(distinct case when nfo.order_id is not null and roi.order_subvertical3_is_ret1 then bp.order_id else null end) as nf_orders_ret,
        count(distinct case when fo.order_id is not null and poi.order_is_pna then bp.order_id else null end) as f_orders_pna,
        count(distinct case when nfo.order_id is not null and poi.order_is_pna then bp.order_id else null end) as nf_orders_pna,
        count(distinct case when fo.order_id is not null then bp.bought_product_id else null end) as f_products,
        count(distinct case when nfo.order_id is not null then bp.bought_product_id else null end) as nf_products
    from  delta.central__bought_products_looker__odp.bought_products bp -- I have used a preworked looker table
    inner join calendar_dates cd
        on bp.p_creation_date = cd.calendar_date
    inner join stores s
        on s.store_address_id = bp.store_address_id
    inner join migrated_stores ms
        on ms.store_address_id = bp.store_address_id
    left join fresh_orders fo
        on bp.order_id = fo.order_id
    left join non_fresh_orders nfo
        on bp.order_id = nfo.order_id
    left join delta.central__retention_orders__odp.retention_order_info roi
        on roi.order_id = bp.order_id
    left join delta.mfc__pna__odp.pna_orders_info poi
        on poi.order_id = bp.order_id
    inner join delta.central_order_descriptors_odp.order_descriptors_v2 od
        on od.order_id = bp.order_id
    where true
        and od.order_parent_relationship_type is null
        and od.order_final_status = 'DeliveredStatus'
    group by 1,2
)

select * from orders