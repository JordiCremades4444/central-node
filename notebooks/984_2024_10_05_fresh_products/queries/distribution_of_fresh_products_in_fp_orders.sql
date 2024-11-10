with calendar_dates as (select
    calendar_date
    from unnest(sequence(date({start_date}), date({end_date}), interval '1' day)) as dates (calendar_date)
    where true
)

,top_brands_last_snapshot as (
    select distinct
        tp.country_code,
        tp.store_name,
        tp.p_ingestion_date
    from delta.mfc__groceries_content_availability_targets__odp.groceries_top_partners tp
    where true
        and tp.p_ingestion_date = (select max(p_ingestion_date) from delta.mfc__groceries_content_availability_targets__odp.groceries_top_partners) 
)

--  pk store_address_id
,stores as (
    select 
        c.country_code, 
        s.store_name,
        s.store_subvertical,
        sa.store_address_id,
        sa.store_id,
        case when tp.store_name is not null then true else false end as is_top_partner
    from delta.partner_stores_odp.store_addresses_v2 sa
    left join delta.partner_stores_odp.stores_v2 s
        on sa.store_id = s.store_id
    left join delta.central_geography_odp.cities_v2 c
        on s.city_code = c.city_code
    left join top_brands_last_snapshot tp
        on tp.store_name = s.store_name
        and tp.country_code = c.country_code
    where true
        and sa.p_end_date is null
        and s.p_end_date is null
        and s.store_vertical = 'QCommerce'
        and s.store_subvertical in ('QCPartners', 'MFC')
        and s.store_subvertical2 = 'Groceries'
        and s.store_is_enabled
        and not (s.store_is_deleted or s.store_is_deleted is null)
        and not (sa.store_address_is_deleted or sa.store_address_is_deleted is null)
        and c.country_code not in ('AR','BO','BY','CL','CO','CR','DO','EC','EG','GT','PE','UY','ZA','TR','PR','BR','HN','PA','FR')
)

, orders_with_fp as (
    select distinct
        bp.order_id as order_id
    from delta.customer_bought_products_odp.bought_products_v2 bp
    inner join calendar_dates cd
        on cd.calendar_date = bp.p_creation_date
    inner join stores s
        on s.store_address_id = bp.store_address_id
    inner join delta.mfc_inventory_odp.products_v2 p
        on bp.store_address_id = p.store_address_id
        and bp.product_external_id = p.product_sku
    where true
        and p.product_category_level_one in ('Produce','Ready to Consume','Meat / Seafood')
)

select 
    s.country_code,        
    -- all groceries
    count(distinct case when store_subvertical = 'QCPartners' then concat(cast(bp.order_id as varchar), '_', cast(bp.product_external_id as varchar)) else null end) as n_pairs_orders_external_id_in_fp_orders_all_groceries,
    count(distinct case 
        when store_subvertical = 'QCPartners' 
        and p.product_category_level_one in ('Produce','Ready to Consume','Meat / Seafood') 
        then concat(cast(bp.order_id as varchar), '_', cast(bp.product_external_id as varchar)) else null end) as n_pairs_orders_fp_external_id_in_fp_orders_all_groceries,

    -- groceries top partnres
    count(distinct case when store_subvertical = 'QCPartners' and is_top_partner then concat(cast(bp.order_id as varchar), '_', cast(bp.product_external_id as varchar)) else null end) as n_pairs_orders_external_id_in_fp_orders_top_partners,
    count(distinct case 
        when store_subvertical = 'QCPartners' and is_top_partner 
        and p.product_category_level_one in ('Produce','Ready to Consume','Meat / Seafood') 
        then concat(cast(bp.order_id as varchar), '_', cast(bp.product_external_id as varchar)) else null end) as n_pairs_orders_fp_external_id_in_fp_orders_top_partners,

    -- MFCs
    count(distinct case when store_subvertical = 'MFC' then concat(cast(bp.order_id as varchar), '_', cast(bp.product_external_id as varchar)) else null end) as n_pairs_orders_external_id_in_fp_orders_mfc,
    count(distinct case 
        when store_subvertical = 'MFC'
        and p.product_category_level_one in ('Produce','Ready to Consume','Meat / Seafood') 
        then concat(cast(bp.order_id as varchar), '_', cast(bp.product_external_id as varchar)) else null end) as n_pairs_orders_fp_external_id_in_fp_orders_mfc
        
from delta.customer_bought_products_odp.bought_products_v2 bp
inner join calendar_dates cd
    on cd.calendar_date = bp.p_creation_date
inner join stores s
    on s.store_address_id = bp.store_address_id
inner join delta.mfc_inventory_odp.products_v2 p
    on bp.store_address_id = p.store_address_id
    and bp.product_external_id = p.product_sku
inner join orders_with_fp owfp
    on owfp.order_id = bp.order_id
group by 1
