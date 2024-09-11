with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date('2024-05-27'),-- initial date
        date('2024-06-02'),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,not_bp_filter_price as (
    select distinct 
        bp.order_id as order_id
    from delta.customer_bought_products_odp.bought_products_v2 bp
    inner join delta.central_order_descriptors_odp.order_descriptors_v2 od
        on od.order_id = bp.order_id
    where 1=1
        and (bp.product_unit_price * od.order_exchange_rate_to_eur ) > 998
        and od.p_creation_date in (select calendar_date from calendar_dates)
) 

, weeks as (
    select
        ogv.order_country_code as country_code,
        week(ogv.p_creation_date) as week_num,
        ogv.order_id as order_id,
        abs(sum(coalesce(ogv.refunded_to_customer_eur,0))) as refunds_to_customer,
        abs(sum(coalesce(ogv.gmv_variation_cx_pna_eur,0))) as cancellations_to_pna,
        sum(coalesce(ogv.gmv_variation_total_removal_eur,0)) + sum(coalesce(ogv.gmv_variation_partial_removal_eur,0)) + sum(coalesce(ogv.gmv_variation_replacement_eur,0)) - abs(sum(coalesce(ogv.gmv_variation_cx_pna_eur,0))) - abs(sum(coalesce(ogv.refunded_to_customer_eur,0))) as gmv_loss_numerator,
        sum(coalesce(ogv.value_original_eur,0)) as gmv_loss_denominator
    from delta.mfc__pna_gmv_variation__odp.orders_gmv_variation as ogv
    inner join delta.central_order_descriptors_odp.order_descriptors_v2 as o
        on ogv.p_creation_date = o.p_creation_date
        and ogv.order_id = o.order_id
    left join delta.partner_stores_odp.stores_v2 s
        on o.store_id = s.store_id
        and s.p_end_date is null
    where 1=1
        and s.store_subvertical = 'QCPartners'
        and s.store_subvertical2 = 'Groceries'
        and ogv.p_creation_date in (select calendar_date from calendar_dates)
        and ogv.order_id not in (select order_id from not_bp_filter_price)
        and ogv.order_country_code = 'ES'
    group by 1,2,3
    order by 1,2
)

select 
    *,
    1.00*gmv_loss_numerator/gmv_loss_denominator as perc_gmv
from weeks
