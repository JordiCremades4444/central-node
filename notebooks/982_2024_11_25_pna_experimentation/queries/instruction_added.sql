with calendar_dates as (
    select 
        calendar_date
    from unnest(sequence(date({start_date}), date({end_date}), interval '1' day)) as cte (calendar_date)
    where true 
)

,custom_event as (
    select
        cu.creation_date,
        cu.event_name,
        cu.custom_attributes__instruction_type,
        cu.custom_attributes__instruction_added,
        cu.custom_attributes__screen,
        count(distinct cu.event_id) as n_events,
        count(distinct cu.customer_id) as n_customers
    from sensitive_delta.customer_mpcustomer_odp.custom_event cu
    inner join calendar_dates cd
        on cd.calendar_date = cu.creation_date
    where true
        and cu.event_name = 'Instruction Added'
    group by 1,2,3,4,5
)

select * from custom_event order by 1,2,3,4 asc

