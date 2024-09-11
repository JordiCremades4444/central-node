select
  c.country_code,
  s.city_code,
  s.store_id,
  s.store_is_in_store_prices_enabled
from delta.partner_stores_odp.stores_v2 s
left join delta.central_geography_odp.cities_v2 c
    on s.city_code = c.city_code
where 1=1
  and s.store_is_in_store_prices_enabled is not null
  and s.p_end_date is null
  and s.city_code in ({cities})