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

, products_per_order as (
    select distinct
        od.order_id,
        count(distinct bp.product_external_id) as uipo
    from delta.central_order_descriptors_odp.order_descriptors_v2 od
    left join delta.customer_bought_products_odp.bought_products_v2 bp
        on od.order_id = bp.order_id
    inner join calendar_dates cd
        on cd.calendar_date = od.p_creation_date    
    inner join stores s
        on od.store_address_id = s.store_address_id
    where true
        and od.order_final_status = 'DeliveredStatus'
    group by 1
)

, all_groceries as (select
        od.order_country_code as country_code,
        s.store_subvertical, 

        --fp ordres
        count(case when owfp.order_id is not null then od.order_id else null end) as n_fp_orders,
        sum(case when owfp.order_id is not null then order_total_effective_purchase_eur else null end) as eur_fp_purchased,
        avg(case when owfp.order_id is not null then uipo else null end) fp_uipo,

        --non fp ordres
        count(case when owfp.order_id is null then od.order_id else null end) as n_non_fp_orders,
        sum(case when owfp.order_id is null then order_total_effective_purchase_eur else null end) as eur_non_fp_purchased,
        avg(case when owfp.order_id is null then uipo else null end) non_fp_uipo,
        
        --total
        count(distinct od.order_id) as n_orders,
        sum(order_total_effective_purchase_eur) as eur_purchased,
        avg(uipo) uipo

    from delta.central_order_descriptors_odp.order_descriptors_v2 od
    inner join calendar_dates cd
        on cd.calendar_date = od.p_creation_date
    inner join stores s
        on od.store_address_id = s.store_address_id
    inner join products_per_order ppo
        on ppo.order_id = od.order_id
    left join orders_with_fp owfp
        on od.order_id = owfp.order_id
    where true
        and od.order_final_status = 'DeliveredStatus'
    group by 1,2
)

, top_partners as (select
        od.order_country_code as country_code,
        'top_partners' as store_subvertical, 

        --fp ordres
        count(case when owfp.order_id is not null then od.order_id else null end) as n_fp_orders,
        sum(case when owfp.order_id is not null then order_total_effective_purchase_eur else null end) as eur_fp_purchased,
        avg(case when owfp.order_id is not null then uipo else null end) fp_uipo,

        --non fp ordres
        count(case when owfp.order_id is null then od.order_id else null end) as n_non_fp_orders,
        sum(case when owfp.order_id is null then order_total_effective_purchase_eur else null end) as eur_non_fp_purchased,
        avg(case when owfp.order_id is null then uipo else null end) non_fp_uipo,
        
        --total
        count(distinct od.order_id) as n_orders,
        sum(order_total_effective_purchase_eur) as eur_purchased,
        avg(uipo) uipo

    from delta.central_order_descriptors_odp.order_descriptors_v2 od
    inner join calendar_dates cd
        on cd.calendar_date = od.p_creation_date
    inner join stores s
        on od.store_address_id = s.store_address_id
    inner join products_per_order ppo
        on ppo.order_id = od.order_id
    left join orders_with_fp owfp
        on od.order_id = owfp.order_id
    where true
        and od.order_final_status = 'DeliveredStatus'
        and s.is_top_partner
    group by 1,2
)

select 
    *
from all_groceries
union all 
select 
    *
from top_partners
