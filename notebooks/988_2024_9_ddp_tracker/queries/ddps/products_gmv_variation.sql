with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date_add('day',-{days_in_advance},current_date),-- initial date
        date_add('day',-1,current_date),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,products_gmv_variation as (
    select 
        p_creation_date
        ,count(distinct bought_product_id) count_products_gmv_variation_products_gmv_variation
    from delta.mfc__pna_gmv_variation__odp.products_gmv_variation
    where true
        and p_creation_date in (select calendar_date from calendar_dates)
    group by 1
)

select 
    *
from products_gmv_variation