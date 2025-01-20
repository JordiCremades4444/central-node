with calendar_dates as (
    select calendar_date
    from unnest(sequence(date({start_date}), date({end_date}), interval '1' day)) as cte (calendar_date)
)

,order_descriptors as (
    select distinct
        order_id
    from delta.central_order_descriptors_odp.order_descriptors_v2
    where store_address_id in (481365, 481369, 481372, 511356, 511457, 543200, 614966, 619585, 638321)
    and date(order_activated_at) in (select calendar_date from calendar_dates)
)

,instructions as (
    select distinct
        order_id
    from delta.mfc__pna_replacement_instructions__odp.pna_replacement_instructions pri
    where true
    and date(p_order_activated_date) in (select calendar_date from calendar_dates)
)

select
    od.order_id as order_id_od,
    pri.order_id as order_id_pri
from order_descriptors od
full outer join instructions pri
    on od.order_id = pri.order_id
where true
