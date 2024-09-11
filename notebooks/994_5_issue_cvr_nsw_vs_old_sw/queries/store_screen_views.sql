with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date_add('day',-{start_previous_to_current_date},current_date),-- initial date
        date_add('day',-{end_previous_to_current_date},current_date),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,store_events as (
    select 
        p_creation_date,
        count(*) as count_events
    from delta.customer_behaviour_odp.enriched_screen_view__store_v3 as stores
    inner join delta.partner_stores_odp.stores_v2
        on stores_v2.p_end_date is null
        and stores_v2.store_subvertical2 = 'Groceries'
        and stores_v2.store_id = stores.store_id
    where 1=1
        and p_creation_date in (select calendar_date from calendar_dates)
    group by 1
)

select * from store_events