with calendar_dates as (
    select calendar_date
    from unnest(sequence(date({start_date}), date({end_date}), interval '1' day)) as cte (calendar_date)
)

select
    p_event_date,
    data__experimentation_variant_value,
    count(distinct data__experimentation_allocation_key)
from delta.mlp_feature_store_experiment_exposure_odp.mlp_experiment_exposure 
inner join calendar_dates cd
    on cd.calendar_date = p_event_date
where true
    and data__experimentation_toggle_id = {experiment_toggle_id}
group by 1, 2
order by 1, 2