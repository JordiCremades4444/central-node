with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date_add('day',-{start_previous_to_current_date},current_date),-- initial date
        date_add('day',-{end_previous_to_current_date},current_date),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,category_events as (
    select 
        p_creation_date,
        count(*) as count_events
    from delta.customer_behaviour_odp.enriched_custom_event__category_opened_v3
    where 1=1
        and p_creation_date in (select calendar_date from calendar_dates)
        and category_id in (4, 540, 622, 679, 682, 762, 875, 1015, 1082, 1195, 1197, 1214, 1314, 1316, 1551, 1718)
    group by 1
)

select * from category_events