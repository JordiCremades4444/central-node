with calendar_dates as (
    select calendar_date
    from unnest(sequence(date({start_date}), date({end_date}), interval '1' day)) as cte (calendar_date)
)

,conflict_customer_id as (
    select distinct customer_id
    from delta.mfc__pna_replacement_instructions__odp.pna_replacement_instructions
    inner join calendar_dates cd
        on cd.calendar_date = date(p_order_activated_date)
    where true
        and ui_variant is null
)

select distinct
    p_event_date,
    data__experimentation_variant_value,
    data__experimentation_allocation_key
from delta.mlp_feature_store_experiment_exposure_odp.mlp_experiment_exposure 
where true
    and data__experimentation_toggle_id = {experiment_toggle_id}
    and cast(data__experimentation_allocation_key as bigint) in (select customer_id from conflict_customer_id)
    and p_event_date >= date '2025-01-01'
