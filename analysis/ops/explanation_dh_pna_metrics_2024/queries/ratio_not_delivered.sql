with calendar_dates as (
    select 
        calendar_date
    from unnest(
        sequence(
            date_add('day',-1000,current_date)
            ,date_add('day',-1,current_date)
            ,interval '1' day
    )) as dates (calendar_date)
    where 1=1
        and date_trunc('month',date(calendar_date)) >= date_add('month', -13, date_trunc('month', date(now())))
        and date_trunc('month',date(calendar_date)) < date_trunc('month',date(now()))
)

,qcparters_orders as (
    select
        od.p_creation_date,
        od.order_id,
        poi.order_is_pna_cancellation
    from delta.central_order_descriptors_odp.order_descriptors_v2 od
    left join delta.mfc__pna__odp.pna_orders_info poi
        on od.order_id = poi.order_id
        and od.p_creation_date = poi.p_creation_date
    where 1=1
        and od.order_parent_relationship_type is null 
        and od.order_subvertical = 'QCPartners'
        and od.p_creation_date in (select calendar_date from calendar_dates)
)

,products as (
    select 
        pgv.order_country_code as order_country_code,
        date_trunc('month',pgv.p_creation_date) as month,
        sum(case
                when replaced_by_bought_product_id is null then 0   
                when replaced_by_bought_product_id is not null and not qo.order_is_pna_cancellation then bought_product_quantity-replacer_bought_product_quantity
            end
        ) as diff_units_removals_and_replacements,
        sum(bought_product_quantity) filter (where not qo.order_is_pna_cancellation) as originally_placed_units_delivered_orders,
        sum(bought_product_quantity) filter (where qo.order_is_pna_cancellation) as originally_placed_units_pna_cancelled_orders
    from delta.mfc__pna_gmv_variation__odp.products_gmv_variation pgv
    inner join qcparters_orders qo
        on pgv.order_id = qo.order_id
        and pgv.p_creation_date = qo.p_creation_date
    where 1=1
        and not pgv.bought_product_id_is_addition 
        and pgv.p_creation_date in (select calendar_date from calendar_dates)
        and pgv.replaced_bought_product_id is null
    group by 1,2
    order by 1 asc, 2 desc
)

select 
    order_country_code,
    month,
    coalesce(diff_units_removals_and_replacements,0) as diff_units_removals_and_replacements,
    coalesce(originally_placed_units_delivered_orders,0) as originally_placed_units_delivered_orders,
    coalesce(originally_placed_units_pna_cancelled_orders,0) as originally_placed_units_pna_cancelled_orders, 
    1.0000*(coalesce(diff_units_removals_and_replacements,0)+coalesce(originally_placed_units_pna_cancelled_orders,0))/(coalesce(originally_placed_units_delivered_orders,0)+coalesce(originally_placed_units_pna_cancelled_orders,0)) as perc_not_delivered
from products