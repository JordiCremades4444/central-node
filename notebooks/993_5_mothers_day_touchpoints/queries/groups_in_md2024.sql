with calendar_dates as (    
    select 
        calendar_date
    from unnest(
        sequence(
            date('2024-05-02') 
            ,date('2024-05-05')
            ,interval '1' day
    )) as dates (calendar_date)
)

,nsw_groups as (
    select distinct variant
    from delta.mlp__experiment_first_exposure__odp.first_exposure
    where 1=1
        and p_first_exposure_date in (select calendar_date from calendar_dates)
        and experiment_toggle_id = 'QC_HOME_STORE_WIDGET_ET'
)

select * from nsw_groups
