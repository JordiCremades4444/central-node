with calendar_dates as (
    select calendar_date
    from unnest(sequence(date({start_date}), date({end_date}), interval '1' day)) as cte (calendar_date)
)

-- =====================================
-- Exposures
-- =====================================

,pna_ui_customer_exposure as (
    select distinct
        fe.allocation_key as customer_id
        ,fe.variant
        ,fe.first_exposure_datetime as start_time
        ,coalesce(lag(fe.first_exposure_datetime) over (partition by fe.allocation_key order by fe.first_exposure_datetime desc), current_timestamp) as end_time
    from delta.mlp__experiment_first_exposure__odp.first_exposure fe
    inner join calendar_dates cd
        on cd.calendar_date = fe.p_first_exposure_date
    where true
        and (fe.experiment_toggle_id = 'ROCKET_PNA_UI_ET')
)

,pna_default_customer_exposure as (
    select distinct
        fe.allocation_key as customer_id
        ,fe.variant
        ,fe.first_exposure_datetime as start_time
        ,coalesce(lag(fe.first_exposure_datetime) over (partition by fe.allocation_key order by fe.first_exposure_datetime desc), current_timestamp) as end_time
    from delta.mlp__experiment_first_exposure__odp.first_exposure fe
    inner join calendar_dates cd
        on cd.calendar_date = fe.p_first_exposure_date
    where true
        and (fe.experiment_toggle_id = 'ROCKET_PNA_DEFAULT_CHOICE_ET')
)

-- =====================================
-- Orders with Instructions
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
-- Enrich with Order information
-- =====================================

,order_enrichment as (
    select
        od.customer_id,
        o.*
    from orders_products_unnested_with_strategy_and_suggestions o
    left join delta.central_order_descriptors_odp.order_descriptors_v2 od
        on od.order_id = o.order_id
)

-- =====================================
-- Enrich with Exposures Variants
-- =====================================

,variant_enrichment as (
    select 
        o.*,
        pna_ui_customer_exposure.variant as ui_variant,
        pna_default_customer_exposure.variant as default_variant
    from order_enrichment o
    left join pna_ui_customer_exposure
        on o.customer_id = pna_ui_customer_exposure.customer_id
        and event_creation_time between pna_ui_customer_exposure.start_time and pna_ui_customer_exposure.end_time 
    left join pna_default_customer_exposure
        on o.customer_id = pna_default_customer_exposure.customer_id
        and event_creation_time between pna_default_customer_exposure.start_time and pna_default_customer_exposure.end_time
)

-- -- =====================================
-- -- Ennrich with Product information
-- -- =====================================

,bought_products_filtered as (
    select 
        bp.p_creation_date,
        bp.order_id,
        bp.product_name,
        bp.product_external_id,
        bp.bought_product_id,
        bp.bought_product_quantity
    from delta.customer_bought_products_odp.bought_products_v2 bp
    inner join calendar_dates
        on calendar_dates.calendar_date = p_creation_date
    where true
        and replaced_bought_product_id is null
)

,product_enrichment as (
    select
        v.*,
        bpf.product_name,
        bpf.bought_product_id,
        bpf.bought_product_quantity
    from variant_enrichment v
    left join bought_products_filtered bpf
        -- adding p_creation_date leads to incorrect
        on bpf.order_id = v.order_id
        and bpf.product_external_id = v.external_id
)

-- -- =====================================
-- -- Ennrich with Replacement information
-- -- =====================================

,replacement_enrichment as (
    select
        p.*,
        bp.product_name as replaced_product_name,
        bp.product_external_id as replaced_external_id,
        bp.bought_product_quantity as replaced_bought_product_quantity
    from product_enrichment p
    left join delta.customer_bought_products_odp.bought_products_v2 bp
        on p.bought_product_id = bp.replaced_bought_product_id
        and p.p_creation_date = bp.p_creation_date
)

select * from replacement_enrichment