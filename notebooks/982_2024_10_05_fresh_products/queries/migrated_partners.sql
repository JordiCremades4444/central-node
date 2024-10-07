with stores as (
    select 
        c.country_code, 
        s.store_subvertical,
        sa.store_address_id,
        sa.store_id
    from delta.partner_stores_odp.store_addresses_v2 sa
    left join delta.partner_stores_odp.stores_v2 s
        on sa.store_id = s.store_id
    left join delta.central_geography_odp.cities_v2 c
        on s.city_code = c.city_code
    where true
        and sa.p_end_date is null
        and s.p_end_date is null
        and s.store_vertical = 'QCommerce'
        and s.store_subvertical in ('QCPartners', 'MFC')
        and s.store_subvertical2 = 'Groceries'
        and s.store_is_enabled
        and not (s.store_is_deleted or s.store_is_deleted is null)
        and not (sa.store_address_is_deleted or sa.store_address_is_deleted is null)
        and country_code not in ('AR','BO','BY','CL','CO','CR','DO','EC','EG','GT','PE','UY','ZA','TR','PR','BR','HN','PA','FR')
)

select
    s.country_code,
    s.store_subvertical,
    count(distinct s.store_id) as n_store_id,
    count(distinct case when p.store_address_id is not null then s.store_id else null end) as n_store_id_migrated,
    1.00*count(distinct case when p.store_address_id is not null then s.store_id else null end)/nullif(count(distinct s.store_id),0) as perc_store_id_migrated
from stores s
left join delta.mfc_inventory_odp.products_v2 p
    on s.store_address_id = p.store_address_id
group by 1,2
order by 1,2,3,4

