with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date({start_date}),-- initial date
        date({end_date}),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,calendar_dates_groups as (select
        calendar_date
    from unnest(sequence(
        date({start_date_groups}),-- initial date
        date({end_date_groups}),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,top_cities as (
    select
        od.order_city_code,
        od.order_country_code,
        count(distinct od.order_id) as n_orders
    from delta.central_order_descriptors_odp.order_descriptors_v2 od
    inner join delta.partner_stores_odp.stores_v2 s
        on od.store_id = s.store_id
        and s.p_end_date is null
    where 1=1
        and od.p_creation_date in (select calendar_date from calendar_dates)
        and s.store_subvertical2 = 'Groceries'
    group by 1,2
    order by 3 desc
    limit {top}
)

,instore_prices_groups as (
    select
        experiment_toggle_id,
        allocation_key as customer_id,
        variant,
        first_exposure_datetime as start_time,
        coalesce(lag(first_exposure_datetime) over (partition by allocation_key order by first_exposure_datetime desc), current_date) as end_time
    from delta.mlp__experiment_first_exposure__odp.first_exposure
    where 1=1
        and p_first_exposure_date in (select calendar_date from calendar_dates_groups)
        and experiment_toggle_id = 'ZAP_NSW_EXPERIMENT'
)

,all_sessions as (
    select
        ds.country_code as country,
        ds.city_code as city,
        ds.p_creation_date, 
        ipg.variant as ipg_variant,
        count(distinct ds.dynamic_session_id) as total_session_count,
        count(distinct ds.customer_id) as total_customers
    from delta.customer_behaviour_odp.dynamic_sessions_v1 ds
    left join instore_prices_groups ipg
        on ipg.customer_id = ds.customer_id
        and ipg.start_time <= ds.session_start_time
        and ipg.end_time > ds.session_start_time
    where 1=1
        and p_creation_date in (select calendar_date from calendar_dates)
        and ds.city_code in (select order_city_code from top_cities)
    group by 1,2,3,4
)

,store_walls_sessions as (
    select 
        co.country,
        co.city,
        co.p_creation_date,
        ipg.variant as ipg_variant,
        count(distinct co.dynamic_session_id) as groceries_sw_session_count
    from delta.customer_behaviour_odp.enriched_custom_event__category_opened_v3 as co
    left join instore_prices_groups ipg
        on ipg.customer_id = co.customer_id
        and ipg.start_time <= co.creation_time
        and ipg.end_time > co.creation_time
    where 1=1
        and co.p_creation_date in (select calendar_date from calendar_dates)
        and co.city in (select order_city_code from top_cities)
        and co.category_tag = 'Groceries'
    group by 1,2,3,4
)

select
    ases.*,
    swses.groceries_sw_session_count
from all_sessions ases
left join store_walls_sessions swses
    on ases.country = swses.country
    and ases.city = swses.city
    and ases.p_creation_date = swses.p_creation_date
    and ases.ipg_variant = swses.ipg_variant


