select distinct 
    csv2.country_code,
    sv2.city_code,
    sv2.store_id
from delta.partner_stores_odp.stores_v2 sv2
left join delta.central_geography_odp.cities_snapshot_v2 csv2
    on csv2.city_code = sv2.city_code
    and csv2.p_snapshot_date = (select max(p_snapshot_date) from delta.central_geography_odp.cities_snapshot_v2)
where 1=1
    and sv2.p_end_date is null
    and sv2.store_is_enabled
    and not sv2.store_is_deleted
    /*
    This is the condition to identify Specialties
    */ 
    and sv2.store_subvertical = 'QCPartners'
    and sv2.store_subvertical3 = 'Groceries'
    and upper(sv2.store_sub_business_unit) not in ('CONVENIENCE', 'SUPERMARKET', 'OTHER')