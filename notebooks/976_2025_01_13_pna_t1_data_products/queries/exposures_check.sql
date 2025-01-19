with calendar_dates as (
    select calendar_date
    from unnest(sequence(date({start_date}), date({end_date}), interval '1' day)) as cte (calendar_date)
)

,pna_ui_customer_exposure as (
    select distinct
        fe.allocation_key as customer_id
        ,fe.variant
        ,fe.first_exposure_datetime as start_time
        ,coalesce(lag(fe.first_exposure_datetime) over (partition by fe.allocation_key order by fe.first_exposure_datetime desc), current_timestamp) as end_time
    from delta.mlp__experiment_first_exposure__odp.first_exposure fe
    inner join calendar_dates cd
        on cd.calendar_date = fe.p_first_exposure_date
    where true
        and (fe.experiment_toggle_id = 'ROCKET_PNA_UI_ET')
)

,pna_default_customer_exposure as (
    select distinct
        fe.allocation_key as customer_id
        ,fe.variant
        ,fe.first_exposure_datetime as start_time
        ,coalesce(lag(fe.first_exposure_datetime) over (partition by fe.allocation_key order by fe.first_exposure_datetime desc), current_timestamp) as end_time
    from delta.mlp__experiment_first_exposure__odp.first_exposure fe
    inner join calendar_dates cd
        on cd.calendar_date = fe.p_first_exposure_date
    where true
        and (fe.experiment_toggle_id = 'ROCKET_PNA_DEFAULT_CHOICE_ET')
)

,exposures_pna_ui as (
    select
        calendar_date,
        count(case when variant = 'Control Group' then customer_id else null end) as n_control_group_ui,
        count(case when variant = 'Cart Variant' then customer_id else null end) as n_variant_ui,
        count(case when variant is null then customer_id else null end) as n_null_variant_ui
    from calendar_dates
    left join pna_ui_customer_exposure
        on calendar_dates.calendar_date between pna_ui_customer_exposure.start_time and pna_ui_customer_exposure.end_time   
    group by 1
)

,exposures_pna_default as (
    select
        calendar_date,
        count(case when variant = 'Control Group' then customer_id else null end) as n_control_group_default,
        count(case when variant = 'variant 1' then customer_id else null end) as n_variant_default,
        count(case when variant is null then customer_id else null end) as n_null_variant_default
    from calendar_dates
    left join pna_default_customer_exposure
        on calendar_dates.calendar_date between pna_default_customer_exposure.start_time and pna_default_customer_exposure.end_time   
    group by 1
)

select 
    calendar_dates.calendar_date,
    exposures_pna_ui.n_control_group_ui,
    exposures_pna_ui.n_variant_ui,
    exposures_pna_ui.n_null_variant_ui,
    exposures_pna_default.n_control_group_default,
    exposures_pna_default.n_variant_default,
    exposures_pna_default.n_null_variant_default
from calendar_dates
left join exposures_pna_ui
    on calendar_dates.calendar_date = exposures_pna_ui.calendar_date
left join exposures_pna_default
    on calendar_dates.calendar_date = exposures_pna_default.calendar_date
where true