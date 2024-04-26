select 
    distinct data__experimentation_allocation_key as customer_id,
    data__experimentation_variant_value as experiment_group
from delta.mlp_feature_store_experiment_exposure_odp.mlp_experiment_exposure
where 1=1
    and p_event_date >= date_add('day', -1,  date('{start_test}'))
    and p_event_date <= date_add('day', 1,  date('{end_test}'))
    and data__experimentation_toggle_id = '{experimentation_toggle_id}'
    and data__experimentation_variant_value in ({test_groups})