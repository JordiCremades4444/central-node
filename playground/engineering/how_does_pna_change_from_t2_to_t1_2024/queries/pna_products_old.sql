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
    count(distinct bought_product_id ) as total_bought_products,
    count_if(bought_product_id_is_pna) as bought_product_id_is_pna,
    count_if(bought_product_id_is_pna_partial_removal) as bought_product_id_is_pna_partial_removal,
    count_if(bought_product_id_is_pna_total_removal) as bought_product_id_is_pna_total_removal,
    count_if(bought_product_id_is_pna_replacement) as bought_product_id_is_pna_replacement,
    count_if(bought_product_id_is_wm_feedback) as bought_product_id_is_wm_feedback
from delta.central__pna_products__odp.pna_products_info_v2
where 1=1
    and p_creation_date in (select calendar_date from calendar_dates)
group by 1