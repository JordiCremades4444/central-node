with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date_add('day',-{days_in_advance},current_date),-- initial date
        date_add('day',-1,current_date),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,pna_products_info as (
    select 
        p_creation_date
        ,count(distinct bought_product_id) count_bought_product_id_pna_products_info
    from delta.mfc__pna__odp.pna_products_info
    where true
        and p_creation_date in (select calendar_date from calendar_dates)
    group by 1
)

select 
    *
from pna_products_info