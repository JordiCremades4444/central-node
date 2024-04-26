with calendar_dates as (select 
        calendar_date
    from unnest(
        sequence(
            date_add('day',-365,current_date)
            ,date_add('day',-2,current_date)
            ,interval '1' day
    )) as dates (calendar_date)
)

select
    date_trunc('month', p_creation_date) as month,
    count(distinct order_id) as total_orders,
    count_if(order_is_pna) as order_is_pna,
    count_if(order_is_pna_replacement) as order_is_pna_replacement,
    count_if(order_is_pna_partial_removal) as order_is_pna_partial_removal,
    count_if(order_is_pna_total_removal) as order_is_pna_total_removal,
    count_if(order_is_pna_wm_feedback) as order_is_pna_wm_feedback,
    count_if(order_is_pna_refund) as order_is_pna_refund,
    count_if(order_is_pna_cancellation) as order_is_pna_cancellation
from delta.central__pna_orders__odp.pna_orders_info_v2
where 1=1
    and p_creation_date in (select calendar_date from calendar_dates)
group by 1