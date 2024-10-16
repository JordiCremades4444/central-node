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

,product_impressions as (
    select distinct
        country,
        dynamic_session_id,
        segment
    from delta.customer_behaviour_odp.enriched_custom_event__product_impression_v3 pi
    inner join calendar_dates cd
        on pi.p_creation_date = cd.calendar_date
    inner join migrated_stores ms
        on ms.store_address_id = pi.store_address_id
    left join delta.mfc_inventory_odp.products_v2 p
        on p.store_address_id = pi.store_address_id
        and p.product_sku = cast(pi.product_external_id as varchar)
        and p.product_category_level_one in ('Produce','Ready to Consume','Meat / Seafood') 
    left join stores
        on stores.store_address_id = pi.store_address_id
    where true
)

,order_created as (
    select distinct
        country,
        dynamic_session_id,
        segment
    from delta.customer_behaviour_odp.enriched_custom_event__order_created_v3 oc
    inner join calendar_dates cd
        on oc.p_creation_date = cd.calendar_date
    inner join migrated_stores ms
        on oc.store_address_id = ms.store_address_id
    left join stores
        on stores.store_address_id = oc.store_address_id
)

select 
    pi.country,
    pi.segment,
    count(distinct pi.dynamic_session_id) as f_sessions,
    count(distinct oc.dynamic_session_id) as o_sessions
from product_impressions pi
left join order_created oc
    on pi.dynamic_session_id = oc.dynamic_session_id
    and pi.segment = oc.segment
group by 1,2





