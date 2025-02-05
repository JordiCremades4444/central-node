with calendar_dates as (select
    calendar_date
    from unnest(sequence(date({start_date}),date({end_date}),interval '1' day)) as dates (calendar_date)
    where true
)

-- Collection Opened to map Store
,collection_opened as (
    select 
        custom_attributes__collection_opened_origin,
        count(distinct event_id) as n_events
    from sensitive_delta.customer_mpcustomer_odp.custom_event cu
    inner join calendar_dates cd
        on cd.calendar_date = cu.creation_date
    where true
        and cu.event_name = 'Collection Opened'
        and cu.custom_attributes__collection_type = 'Catalogue'
    group by 1
)

select * from collection_opened