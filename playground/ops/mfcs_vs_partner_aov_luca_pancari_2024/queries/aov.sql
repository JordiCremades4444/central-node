with calendar_dates as (select 
        calendar_date
    from unnest(
        sequence(
            date_add('day',-1,date('2023-11-28'))
            ,date_add('day',+1,date('2024-01-27'))
            ,interval '1' day
    )) as dates (calendar_date)
)

,customer_groups as (
    select distinct
        data__experimentation_allocation_key as customer_id,
        data__experimentation_variant_value as experiment_group
from delta.mlp_feature_store_experiment_exposure_odp.mlp_experiment_exposure
where 1=1
    and p_event_date in (select calendar_date from calendar_dates)
    and data__experimentation_toggle_id = 'LGSD_MFC_OR_PARTNERS_ET'
    and data__experimentation_variant_value in ('Test Group','Control Group')
)

, order_descriptors as (
    select
        od.order_city_code as city,
        coalesce(cg.experiment_group, 'out of experiment') as experiment_group,
        od.order_id,
        od.order_final_status,
        od.customer_id,
        od.order_total_purchase_eur,
        ogv.value_original_eur,
        ogv.value_final_eur
from delta.central_order_descriptors_odp.order_descriptors_v2 od
left join customer_groups cg
    on od.customer_id = cg.customer_id
    --and od.p_creation_date = p_event_date
left join delta.mfc__pna_gmv_variation__odp.orders_gmv_variation ogv
    on od.order_id = ogv.order_id
where 1=1
    and od.p_creation_date in (select calendar_date from calendar_dates)
    and ogv.p_creation_date in (select calendar_date from calendar_dates)
    and od.order_city_code in  ('LIS', 'BUC', 'BCN', 'ZAG', 'BEG')
    and od.order_subvertical2 = 'Groceries'
    and od.order_parent_relationship_type is null
)

select 
    *
from order_descriptors