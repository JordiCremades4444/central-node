select
    order_id,
    gmv_variation_additions_local,
    gmv_variation_total_removal_local,
    gmv_variation_partial_removal_local,
    gmv_variation_replacement_local,
    gmv_variation_cx_pna_local,
    refunded_to_customer_local,
    gmv_variation_additions_eur,
    gmv_variation_total_removal_eur,
    gmv_variation_partial_removal_eur,
    gmv_variation_replacement_eur,
    gmv_variation_cx_pna_eur,
    refunded_to_customer_eur
from delta.mfc__pna_gmv_variation__odp.orders_gmv_variation
where 1=1
    and order_id in (
    100380267191,100365649314,100399726924,100400421244,100337438447   
    ,100347902252,100348456378,100347588028,100348056064,100348420516
    ,100375914105,100389275524,100345382266,100346274072,100361213045
    ,100380590636,100396193925,100395495892,100395502068,100395688663
    ,100404806178,100404775887,100405019297,100404981087,100404508168
    ,100364610738,100364102442,100404555503,100405228398,100405613920
    )