with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date_add('day',-0,date({start_date})),-- initial date
        date_add('day',-0,date({end_date})),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,do_not_consider_groups as (
    select
        distinct data__experimentation_allocation_key as customer_id
    from delta.mlp_feature_store_experiment_exposure_odp.mlp_experiment_exposure
    where 1=1
        and p_event_date in (select calendar_date from calendar_dates)
        and data__experimentation_toggle_id in ('ZAP_CATEGORY_LANDING_PAGE', 'ZAP_NSW_EXPERIMENT')
        and data__experimentation_variant_value in (
            'forced_assignment' -- nsw exception
            ,'control_outofrange','forced_assignment_1','control_ft','control_ft' --instore prices experiment
        )
)

,nsw_groups as (
    select
        experiment_toggle_id,
        allocation_key as customer_id,
        variant,
        first_exposure_datetime as start_time,
        coalesce(lag(first_exposure_datetime) over (partition by allocation_key order by first_exposure_datetime desc), current_date) as end_time
    from delta.mlp__experiment_first_exposure__odp.first_exposure
    where 1=1
        and p_first_exposure_date in (select calendar_date from calendar_dates)
        and experiment_toggle_id = 'ZAP_CATEGORY_LANDING_PAGE'
        and allocation_key not in (select customer_id from do_not_consider_groups)
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
        and p_first_exposure_date in (select calendar_date from calendar_dates)
        and experiment_toggle_id = 'ZAP_NSW_EXPERIMENT'
        and allocation_key not in (select customer_id from do_not_consider_groups)
)

,orders as (
    select 
        p_creation_date,
        order_created_at,
        country_code,
        customer_id,
        order_id,
        order_total_purchase_eur
    from delta.central_order_descriptors_odp.order_descriptors_v2
    where 1=1
        and p_creation_date in (select calendar_date from calendar_dates)
        and customer_id not in(select customer_id from do_not_consider_groups)
        and order_subvertical = 'QCPartners'
        and order_subvertical2 = 'Groceries'
)

select 
    nswg.variant as nswg_variant,
    ipg.variant as ipg_variant,
    h.p_creation_date,
    count(distinct o.order_id) as n_orders,
    sum(distinct o.order_total_purchase_eur) as aov,
    count(*) as n_rows
from orders o
left join nsw_groups nswg
    on nswg.customer_id = o.customer_id
    and nswg.start_time <= o.order_created_at
    and nswg.end_time > o.order_created_at
left join instore_prices_groups ipg
    on ipg.customer_id = o.customer_id
    and ipg.start_time <= o.order_created_at
    and ipg.end_time > o.order_created_at
group by 1,2,3