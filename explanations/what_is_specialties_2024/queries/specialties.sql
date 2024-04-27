select
    sv2.store_id
from delta.partner_stores_odp.stores_v2 sv2
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