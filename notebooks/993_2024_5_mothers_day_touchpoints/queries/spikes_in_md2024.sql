with calendar_dates as (    
    select 
        calendar_date
    from unnest(
        sequence(
            date('2024-01-01') 
            ,date('2025-05-01')
            ,interval '1' day
    )) as dates (calendar_date)
)

,nsw_groups as (
    select
        p_first_exposure_date,
        count(*)
    from delta.mlp__experiment_first_exposure__odp.first_exposure
    where 1=1
        and p_first_exposure_date in (select calendar_date from calendar_dates)
        and experiment_toggle_id = 'QC_HOME_STORE_WIDGET_ET'
    group by 1
)

select * from nsw_groups
