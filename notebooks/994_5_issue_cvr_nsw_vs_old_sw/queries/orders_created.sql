with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date_add('day',-{start_previous_to_current_date},current_date),-- initial date
        date_add('day',-{end_previous_to_current_date},current_date),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,orders_created as (
    select 
        p_creation_date,
        count(*) as count_events
    from delta.customer_behaviour_odp.enriched_backend_event__checkout_order_created_v3
    where 1=1
        and p_creation_date in (select calendar_date from calendar_dates)
    group by 1
)

select * from orders_created