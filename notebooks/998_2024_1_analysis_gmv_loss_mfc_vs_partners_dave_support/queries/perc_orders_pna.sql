with customer_groups as (
    select 
    distinct data__experimentation_allocation_key as customer_id,
    data__experimentation_variant_value as experiment_group
from delta.mlp_feature_store_experiment_exposure_odp.mlp_experiment_exposure
where 1=1
    and p_event_date >= date_add('day', -1,  date('{start_test}'))
    and p_event_date <= date_add('day', 1,  date('{end_test}'))
    and data__experimentation_toggle_id = '{experimentation_toggle_id}'
    and data__experimentation_variant_value in ({test_groups})
)

select 
    p_creation_date,
    order_city_code,
    experiment_group,
    count(*) as total_rows,
    count(distinct order_id) as total_orders,
    count_if(order_is_pna) as total_orders_with_pna 
from delta.mfc__pna__odp.pna_orders_info as pna_orders_info
left join delta.partner_stores_odp.store_addresses_v2  as sa
    on sa.store_address_id = pna_orders_info.store_address_id
    and sa.p_end_date is null
left join delta.partner_stores_odp.stores_v2 as s
    on s.store_id = sa.store_id
    and s.p_end_date is null
inner join customer_groups as customer_groups
    on customer_groups.customer_id = pna_orders_info.customer_id
where 1=1
    and p_creation_date >= date_add('day', -{more_less_days},  date('{start_test}'))
    and p_creation_date <= date_add('day', {more_less_days},  date('{end_test}'))
    and order_city_code in ({cities})
    and store_subvertical2 = 'Groceries'
group by 1,2,3