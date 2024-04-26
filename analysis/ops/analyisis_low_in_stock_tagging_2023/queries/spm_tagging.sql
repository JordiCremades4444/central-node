--v1
with first_date as (select 
        date(min(prediction_time)) as first_date_spm
    from delta.desert_public_raw_events.mlp_missing_items_groceries_stock_prediction_model_output_v0__mlp_models_qcommerce_data_missingitemsgroceriesstockpredictionmodeloutput
)

-- pk bought_product_id
,bought_products as (select distinct
        o.order_activated_at,
        concat(cast(bp.product_external_id as varchar),'_', cast(bp.store_address_id as varchar)) as external_id_store_address_id,
        bp.bought_product_id,
        bought_product_id_is_pna
    from delta.central_order_descriptors_odp.order_descriptors_v2 o
    inner join delta.customer_bought_products_odp.bought_products bp
        on o.order_id = bp.order_id
    left join delta.central__pna_products__odp.pna_products_info_v2 pna
        on bp.bought_product_id = pna.bought_product_id
        and pna.p_creation_date >= (select first_date_spm from first_date)
    where 1=1
        and o.p_creation_date >= (select first_date_spm from first_date) -- when the model started working
        and o.order_vertical = 'QCommerce'
        and o.order_subvertical = 'QCPartners'
        and o.order_final_status = 'DeliveredStatus'
)

-- pk prediciton_time + external_id_store_address_id
,spm_info as (select
        prediction_time,
        concat(cast(product_id_partner as varchar),'_', cast(store_address_id as varchar)) as external_id_store_address_id,
        prob as pna_probability
    from delta.desert_public_raw_events.mlp_missing_items_groceries_stock_prediction_model_output_v0__mlp_models_qcommerce_data_missingitemsgroceriesstockpredictionmodeloutput 
)

-- pk prediction_time + external_id_store_address_id
,spm_info_calendar as (select
        *,
        prediction_time as start_event,
        coalesce((lead(prediction_time) over(partition by external_id_store_address_id order by prediction_time asc)),current_timestamp) as end_event 
    from spm_info
)

select 
    bp.*,
    i.pna_probability,
    i.prediction_time,
    i.start_event,
    i.end_event
from bought_products bp
left join spm_info_calendar i
    on bp.external_id_store_address_id = i.external_id_store_address_id
    and bp.order_activated_at >= i.start_event
    and bp.order_activated_at < i.end_event
--endv1

--v2
with first_date as (select 
        date(min(prediction_time)) as first_date_spm
    from delta.desert_public_raw_events.mlp_missing_items_groceries_stock_prediction_model_output_v0__mlp_models_qcommerce_data_missingitemsgroceriesstockpredictionmodeloutput
)

-- pk bought_product_id
,bought_products as (select distinct
        o.order_activated_at,
        concat(cast(bp.product_external_id as varchar),'_', cast(bp.store_address_id as varchar)) as external_id_store_address_id,
        bp.bought_product_id,
        bought_product_id_is_pna,
        bp.product_unit_price*bp.bought_product_quantity*o.order_exchange_rate_to_eur as bought_proudct_id_value_eur
    from delta.central_order_descriptors_odp.order_descriptors_v2 o
    inner join delta.customer_bought_products_odp.bought_products bp
        on o.order_id = bp.order_id
    left join delta.central__pna_products__odp.pna_products_info_v2 pna
        on bp.bought_product_id = pna.bought_product_id
        and pna.p_creation_date >= (select first_date_spm from first_date)
    where 1=1
        and o.p_creation_date >= (select first_date_spm from first_date) -- when the model started working
        and o.order_vertical = 'QCommerce'
        and o.order_subvertical = 'QCPartners'
        and o.order_final_status = 'DeliveredStatus'
        and bp.replaced_bought_product_id is null
)

-- pk prediciton_time + external_id_store_address_id
,spm_info as (select
        prediction_time,
        concat(cast(product_id_partner as varchar),'_', cast(store_address_id as varchar)) as external_id_store_address_id,
        prob as pna_probability
    from delta.desert_public_raw_events.mlp_missing_items_groceries_stock_prediction_model_output_v0__mlp_models_qcommerce_data_missingitemsgroceriesstockpredictionmodeloutput 
)

-- pk prediction_time + external_id_store_address_id
,spm_info_calendar as (select
        *,
        prediction_time as start_event,
        coalesce((lead(prediction_time) over(partition by external_id_store_address_id order by prediction_time asc)),current_timestamp) as end_event 
    from spm_info
)

select 
    bp.*,
    i.pna_probability,
    i.prediction_time,
    i.start_event,
    i.end_event
from bought_products bp
left join spm_info_calendar i
    on bp.external_id_store_address_id = i.external_id_store_address_id
    and bp.order_activated_at >= i.start_event
    and bp.order_activated_at < i.end_event
--endv2