with calendar_dates as (
    select
        calendar_date
    from unnest(sequence(date{start_date}, date{end_date}, interval '1' day)) as cte (calendar_date)
    where true
)

select 
    experiment_toggle_id,
    experiment_id,
    min(p_first_exposure_date) as first_exposure_date
from delta.mlp__experiment_first_exposure__odp.first_exposure as e
inner join calendar_dates cd
    on cd.calendar_date = e.p_first_exposure_date
where true
    and experiment_toggle_id in ('ZAP_SURFACING_PROMOS','SONIC_SURFACING_PROMOS_INLINECAROUSEL',' SONIC_SURFACING_PROMOS_ENTRY_POINTS_')
group by 1,2
order by 1,2,3 asc
