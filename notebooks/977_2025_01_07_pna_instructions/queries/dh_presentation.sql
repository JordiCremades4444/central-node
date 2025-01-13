with calendar_dates as (
    select calendar_date
    from unnest(sequence(date({start_date}), date({end_date}), interval '1' day)) as cte (calendar_date)
)

-- =====================================
-- Customer Exposure
-- =====================================

,glovo_customers as (
    select
        u.user_id as customer_id
    from delta.central_users_odp.users_v2 u
    where true
        and not user_is_staff
        and not user_is_glovo_employee
        and user_type = 'Customer'
)

,customer_exposure as (
    select distinct
        fe.allocation_key as customer_id
        ,fe.variant
        ,fe.first_exposure_datetime as start_time
        ,coalesce(lag(fe.first_exposure_datetime) over (partition by fe.allocation_key order by fe.first_exposure_datetime desc), current_timestamp) as end_time
    from delta.mlp__experiment_first_exposure__odp.first_exposure fe
    inner join glovo_customers gc
        on fe.allocation_key = gc.customer_id
    inner join calendar_dates cd
        on cd.calendar_date = fe.p_first_exposure_date
    where true
        and (fe.experiment_toggle_id = 'ROCKET_PNA_DEFAULT_CHOICE_ET')
)

-- =====================================
-- Decouple Luans Table
-- =====================================


,last_entry_orders as (
    select
        orderid as order_id,
        products,
        p_ingestion_date as p_creation_date,
        event_creation_time,
        row_number() over (partition by orderid order by event_creation_time desc) as rank
    from delta.tech__partner_order_analytics_order_dispatched_with_pna_v0__odp.partner_orders_orderdispatchedtopartnerwithpnaanalyticsevent oi
    inner join calendar_dates on calendar_dates.calendar_date = oi.p_ingestion_date
    where true
)

,orders as (
    select
        order_id,
        products,
        p_creation_date,
        event_creation_time
    from last_entry_orders
    where rank = 1
)

,orders_product_unnested as (
    select 
        order_id,
        p_creation_date,
        event_creation_time,
        externalid as external_id,
        cast(unavailabilitystrategy as json) as unavailabilitystrategy
    from orders
    cross join unnest(products) as t
)
 
,orders_product_unnested_with_strategy_and_suggestions_raw as (
    select 
        order_id,
        p_creation_date,
        event_creation_time,
        external_id,
        json_extract_scalar(unavailabilitystrategy, '$.action') as strategy,
        json_extract(unavailabilitystrategy, '$.alternativeproducts') as alternativeproducts_raw
    from orders_product_unnested
)

,orders_products_unnested_with_strategy_and_suggestions as (
    select
        order_id,
        p_creation_date,
        event_creation_time,
        external_id,
        strategy,
        array_agg(json_extract_scalar(value, '$.externalid')) as alternativeproducts
    from orders_product_unnested_with_strategy_and_suggestions_raw
    left join unnest(cast(alternativeproducts_raw as array(json))) as t(value) on true
    group by 1,2,3,4,5
)

-- =====================================
-- Suggestions and Variant
-- =====================================

,suggestion_and_variant as (
    select distinct
        -- orders_preoudcts_unnested_with_strategy_and_suggestions s
        s.p_creation_date,
        s.order_id,
        s.external_id,
        s.strategy,
        s.alternativeproducts,
        -- odelta.central_order_descriptors_odp.order_descriptors_v2 od
        cast(od.customer_id as varchar) as customer_id,
        od.store_address_id,
        od.store_name,
        od.order_subvertical3,
        -- customer_exposure ce
        ce.variant
    from orders_products_unnested_with_strategy_and_suggestions s
    inner join delta.central_order_descriptors_odp.order_descriptors_v2 od 
        on od.order_id = s.order_id
        and od.p_creation_date = s.p_creation_date
        and od.store_address_id in ({store_addresses_id}) -- Filter only for Eataly
    left join customer_exposure ce
        on od.customer_id = ce.customer_id
        and s.event_creation_time between ce.start_time and ce.end_time
    where true
        and s.external_id is not null
)

,suggestion_and_variant_pna_enriched as (
    select
        s.*,
        pna.product_name,
        pna.bought_product_id_is_pna_replacement,
        pna.bought_product_id_is_pna_partial_removal,
        pna.bought_product_id_is_pna_total_removal,
        pna.bought_product_id_is_wm_feedback,
        pna.bought_product_id_is_pna
    from suggestion_and_variant s
    left join delta.logistics__product_not_available__odp.product_not_available pna
        on pna.p_creation_date = s.p_creation_date
        and pna.order_id = s.order_id
        and pna.product_external_id = s.external_id
        and pna.replaced_bought_product_id is null
)

select * from suggestion_and_variant_pna_enriched