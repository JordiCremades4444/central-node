with orders as (
    select
        order_id as orders_order_id,
        customer_id,
        store_address_id,
        order_vertical,
        order_city_code,
        order_country_code,
        order_final_status,
        order_cancel_reason,
        order_parent_relationship_type,
        order_exchange_rate_to_eur
    from delta.central_order_descriptors_odp.order_descriptors_v2
    where 1=1
)

,orders_with_refund as (
    select
        order_id,
        sum(refunded_to_customer) as refunded_to_customer_local,
        sum(refunded_to_customer_eur) as refunded_to_customer_eur
    from delta.contact_order_refund_incidents_odp.order_refund_incidents
    where 1=1
        and reason in ('MISSING_PRODUCTS','WRONG_PRODUCTS') 
    group by 1
)

,bought_products as (
    select
        bought_products.p_creation_date,
        bought_products.order_id,
        --bought_product_id
        bought_products.bought_product_id,
        bought_products_additions_removals_replacements.bought_product_id as replacer_bought_product_id,
        --external_id
        bought_products.product_external_id,
        bought_products_additions_removals_replacements.product_external_id as replacer_product_external_id,
        --name
        bought_products.product_name,
        bought_products_additions_removals_replacements.product_name as replacer_product_name,
        --replaced info
        bought_products.replaced_bought_product_id,
        bought_products.replaced_by_bought_product_id,
        --quantity
        bought_products.bought_product_quantity,
        bought_products_additions_removals_replacements.bought_product_quantity as replacer_bought_product_quantity,
        --product_unit_price
        bought_products.product_unit_price,
        bought_products_additions_removals_replacements.product_unit_price as replacer_product_unit_price,
        -- bought_product_id_is_pna_replacement
        if(bought_products.bought_product_quantity > 0
            and bought_products.replaced_by_bought_product_id is not null
            and bought_products_additions_removals_replacements.product_external_id != bought_products.product_external_id, true, false) as bought_product_id_is_pna_replacement,
        -- bought_product_id_is_pna_partial_removal
        if(bought_products.bought_product_quantity > 0
            and bought_products.replaced_by_bought_product_id is not null
            and bought_products_additions_removals_replacements.bought_product_quantity < bought_products.bought_product_quantity
            and bought_products_additions_removals_replacements.bought_product_quantity > 0
            and bought_products_additions_removals_replacements.product_external_id = bought_products.product_external_id, true, false) as bought_product_id_is_pna_partial_removal,
        -- bought_product_id_is_pna_total_removal
        if(bought_products.bought_product_quantity > 0
           and bought_products.replaced_by_bought_product_id is not null
           and bought_products_additions_removals_replacements.bought_product_quantity = 0
           and bought_products_additions_removals_replacements.product_external_id = bought_products.product_external_id, true, false) as bought_product_id_is_pna_total_removal,
        -- bought_product_id_is_addition
        if(bought_products.replaced_by_bought_product_id is not null
           and bought_products_additions_removals_replacements.bought_product_quantity > bought_products.bought_product_quantity
           and bought_products_additions_removals_replacements.product_external_id = bought_products.product_external_id, true, false) as bought_product_id_is_addition
    from delta.customer_bought_products_odp.bought_products_v2 as bought_products
    left join delta.customer_bought_products_odp.bought_products_v2 as bought_products_additions_removals_replacements
            on bought_products.replaced_by_bought_product_id = bought_products_additions_removals_replacements.bought_product_id
    where 1=1
)

,bought_products_enriched as (
    select
        bought_products.p_creation_date,
        bought_products.order_id,
        order_final_status,
        order_cancel_reason,
        orders.customer_id,
        orders.store_address_id,
        orders.order_vertical,
        orders.order_city_code,
        orders.order_country_code,
        orders.order_exchange_rate_to_eur,
        orders.order_parent_relationship_type,
        bought_products.bought_product_id,
        bought_products.replacer_bought_product_id,
        bought_products.replaced_by_bought_product_id,
        bought_products.replaced_bought_product_id,
        bought_products.bought_product_id_is_pna_replacement,
        bought_products.bought_product_id_is_pna_partial_removal,
        bought_products.bought_product_id_is_pna_total_removal,
        bought_product_id_is_addition,
        --original product
        bought_products.product_external_id,
        bought_products.product_name,
        bought_products.bought_product_quantity,
        bought_products.product_unit_price,
        --replacer product
        bought_products.replacer_product_external_id,
        bought_products.replacer_product_name,
        bought_products.replacer_bought_product_quantity,
        bought_products.replacer_product_unit_price,
        --value original local
        if((order_parent_relationship_type <> 'SPLIT' or order_parent_relationship_type is null)
            and replaced_bought_product_id is null, bought_product_quantity*product_unit_price, null) as value_original_local,
        --value final local
        if((order_parent_relationship_type <> 'SPLIT' or order_parent_relationship_type is null) 
            and replaced_bought_product_id is null, coalesce(replacer_bought_product_quantity*replacer_product_unit_price,bought_product_quantity*product_unit_price), null) as value_final_local,
        --value original eur
        if((order_parent_relationship_type <> 'SPLIT' or order_parent_relationship_type is null)
            and replaced_bought_product_id is null, bought_product_quantity*product_unit_price*order_exchange_rate_to_eur, null) as value_original_eur,
        --value final eur
        if((order_parent_relationship_type <> 'SPLIT' or order_parent_relationship_type is null) 
            and replaced_bought_product_id is null, coalesce(replacer_bought_product_quantity*replacer_product_unit_price*order_exchange_rate_to_eur,bought_product_quantity*product_unit_price*order_exchange_rate_to_eur), null) as value_final_eur
    from bought_products
    left join orders on bought_products.order_id = orders.orders_order_id
)

,gmv_product_level as (
    select
        *,
        --gmv added
        if(bought_product_id_is_addition,value_final_local,null) as value_gmv_addition_local,
            if(bought_product_id_is_addition,value_final_eur,null) as value_gmv_addition_eur,
        --gmv saved or added
        if(bought_product_id_is_pna_partial_removal,value_final_local,null) as value_gmv_saved_partial_removal_local,
        if(bought_product_id_is_pna_partial_removal,value_final_eur,null) as value_gmv_saved_partial_removal_eur,
        if(bought_product_id_is_pna_replacement,value_final_local,null) as value_gmv_saved_replacement_local,
        if(bought_product_id_is_pna_replacement,value_final_eur,null) as value_gmv_saved_replacement_eur,
        --gmv variation
        if(bought_product_id_is_addition,value_final_local-value_original_local,null) as gmv_variation_additions_local,
        if(bought_product_id_is_addition,value_final_eur-value_original_eur,null) as gmv_variation_additions_eur,
        if(bought_product_id_is_pna_total_removal,value_final_local-value_original_local,null) as gmv_variation_total_removal_local,
        if(bought_product_id_is_pna_total_removal,value_final_eur-value_original_eur,null) as ggmv_variation_total_removal_eur,
        if(bought_product_id_is_pna_partial_removal,value_final_local-value_original_local,null) as gmv_variation_partial_removal_local,
        if(bought_product_id_is_pna_partial_removal,value_final_eur-value_original_eur,null) as gmv_variation_partial_removal_eur,
        if(bought_product_id_is_pna_replacement,value_final_local-value_original_local,null) as gmv_variation_replacement_local,
        if(bought_product_id_is_pna_replacement,value_final_eur-value_original_eur,null) as gmv_variation_replacement_eur
    from bought_products_enriched
    where 1=1
)

,gmv_loss_order_from_products as (
    select
        p_creation_date,
        order_id,
        order_final_status,
        order_cancel_reason,
        customer_id,
        store_address_id,
        order_vertical,
        order_city_code,
        order_country_code,
        order_exchange_rate_to_eur,
        order_parent_relationship_type,
        --gmv added
        if(order_final_status='DeliveredStatus',sum(value_gmv_addition_local),null) as value_gmv_addition_local,
        if(order_final_status='DeliveredStatus',sum(value_gmv_addition_eur),null) as value_gmv_addition_eur,
        --gmv saved or added
        if(order_final_status='DeliveredStatus',sum(value_gmv_saved_partial_removal_local),null) as value_gmv_saved_partial_removal_local,
        if(order_final_status='DeliveredStatus',sum(value_gmv_saved_partial_removal_eur),null) as value_gmv_saved_partial_removal_eur,
        if(order_final_status='DeliveredStatus',sum(value_gmv_saved_replacement_local),null) as value_gmv_saved_replacement_local,
        if(order_final_status='DeliveredStatus',sum(value_gmv_saved_replacement_eur),null) as value_gmv_saved_replacement_eur,
        --gmv loss
        if(order_final_status='DeliveredStatus',sum(gmv_variation_additions_local),null) as gmv_variation_additions_local,
        if(order_final_status='DeliveredStatus',sum(gmv_variation_additions_eur),null) as gmv_variation_additions_eur,
        if(order_final_status='DeliveredStatus',sum(gmv_variation_total_removal_local),null) as gmv_variation_total_removal_local,
        if(order_final_status='DeliveredStatus',sum(ggmv_variation_total_removal_eur),null) as ggmv_variation_total_removal_eur,
        if(order_final_status='DeliveredStatus',sum(gmv_variation_partial_removal_local),null) as gmv_variation_partial_removal_local,
        if(order_final_status='DeliveredStatus',sum(gmv_variation_partial_removal_eur),null) as gmv_variation_partial_removal_eur,
        if(order_final_status='DeliveredStatus',sum(gmv_variation_replacement_local),null) as gmv_variation_replacement_local,
        if(order_final_status='DeliveredStatus',sum(gmv_variation_replacement_eur),null) as gmv_variation_replacement_eur,
        --cx due to pna
        if(order_final_status='CanceledStatus' and order_cancel_reason='PRODUCTS_NOT_AVAILABLE',sum(value_original_local),null) as gmv_variation_pna_cx_local,
        if(order_final_status='CanceledStatus' and order_cancel_reason='PRODUCTS_NOT_AVAILABLE',sum(value_original_eur),null) as gmv_variation_pna_cx_eur
    from gmv_product_level
    group by 1,2,3,4,5,6,7,8,9,10,11
)

select 
    gmv_loss_order_from_products.*,
    refunded_to_customer_local,
    refunded_to_customer_eur
from orders
left join gmv_loss_order_from_products on orders.orders_order_id = gmv_loss_order_from_products.order_id
left join orders_with_refund on orders.orders_order_id = orders_with_refund.order_id
