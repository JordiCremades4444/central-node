with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date({start_date}),-- initial date
        date({end_date}),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,do_not_consider_customers as (
    select
        user_id as customer_id
    from delta.central_users_odp.users_v2
    where 1=1
        and not user_is_staff
        and not user_is_glovo_employee
        and user_type = 'Customer'
)

,customer_and_stores_with_at_least_one_loyalty_card as (
    select distinct 
        customer_id,
        store_id
    from delta.mfc__temp_order_loyalty_cards__odp.temp_order_loyalty_cards_enriched
    where 1=1
        and p_creation_date in (select calendar_date from calendar_dates)
        and loyalty_card is not null
)

,first_customer_exposure as (
    select
        s.customer_id,
        min(p_creation_date) as first_exposure_date
    from delta.customer_behaviour_odp.enriched_screen_view__store_v3 s 
    inner join customer_and_stores_with_at_least_one_loyalty_card lc
        on s.customer_id = lc.customer_id
        and s.store_id = lc.store_id
    where 1=1
        and p_creation_date in (select calendar_date from calendar_dates)
    group by 1
)

,observations as (
    select
        s.customer_id,
        s.p_creation_date as p_event_date,
        coalesce(count(distinct od.order_id), 0) as n_orders,
        coalesce(count(distinct lce.loyalty_card), 0) as n_orders_with_loyalty,
        coalesce(count(distinct s.event_id), 0) as n_store_views
    from delta.customer_behaviour_odp.enriched_screen_view__store_v3 s
    left join delta.central_order_descriptors_odp.order_descriptors_v2 od
        on s.customer_id = od.customer_id
        and s.p_creation_date = od.p_creation_date
        and od.order_subvertical2 = 'Groceries'
        and od.order_subvertical  ='QCPartners'
    left join delta.mfc__temp_order_loyalty_cards__odp.temp_order_loyalty_cards_enriched lce
        on od.customer_id = lce.customer_id
        and od.p_creation_date = lce.p_creation_date
    left join first_customer_exposure fce
        on od.customer_id = fce.customer_id
    where 1=1
        and s.store_id in (select store_id from customer_and_stores_with_at_least_one_loyalty_card)
        and s.p_creation_date in (select calendar_date from calendar_dates)
    group by 1,2
    order by 1,2
)

,observations_unnested as (select
    t1.customer_id,
    t1.p_event_date,
    t2.obs_name as observation_name,
    t2.obs_value as observation_value
from observations t1
cross join unnest (
    array['n_orders', 'n_orders_with_loyalty', 'n_store_views'],
    array[n_orders, n_orders_with_loyalty, n_store_views]
) t2 (obs_name, obs_value)
)

,observations_unnested_enriched as (
    select
        fce.customer_id,
        fce.first_exposure_date,
        obs.p_event_date,
        observation_name,
        observation_value
    from first_customer_exposure fce
    left join observations_unnested obs
        on fce.customer_id = obs.customer_id
    where 1=1
        and obs.observation_value != 0
        and p_event_date > first_exposure_date or p_event_date is null
)

,observations_with_exposure_date as (
    select
        *,
        cast(floor(date_diff('day', (select min(calendar_date) from calendar_dates), p_event_date))/7 as int) + 1 as obs_week_index_since_start_date,
        cast(floor(date_diff('day', (select min(calendar_date) from calendar_dates), first_exposure_date))/7 as int) + 1 as exposure_week_index_since_start_date
    from observations_unnested_enriched
)

,observations_with_exposure_date_cumsum as (
    select
        customer_id,
        observation_name,
        first_exposure_date,
        exposure_week_index_since_start_date,
        obs_week_index_since_start_date,
        sum(sum(observation_value)) over (partition by customer_id, first_exposure_date, exposure_week_index_since_start_date, observation_name order by obs_week_index_since_start_date) as cumsum_observation_value
    from observations_with_exposure_date
    where 1=1
        and customer_id is not null
    group by 1,2,3,4,5
)

select
    o.customer_id,
    o.first_exposure_date,
    o.exposure_week_index_since_start_date,
    o.obs_week_index_since_start_date,
    sum(case when observation_name = 'n_orders' then cumsum_observation_value else 0 end) as n_orders,
    sum(case when observation_name = 'n_orders_with_loyalty' then cumsum_observation_value else 0 end) as n_orders_with_loyalty,
    sum(case when observation_name = 'n_store_views' then cumsum_observation_value else 0 end) as n_store_views
from observations_with_exposure_date_cumsum o
inner join do_not_consider_customers donot
    on o.customer_id = donot.customer_id
group by 1,2,3,4
