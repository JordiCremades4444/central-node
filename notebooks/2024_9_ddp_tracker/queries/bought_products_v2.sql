with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date_add('day',-{days_in_advance},current_date),-- initial date
        date_add('day',-1,current_date),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,bought_products_v2 as (
    select 
        p_creation_date
        ,count(distinct bought_product_id) count_bought_proudct_id_bought_products_v2
    from delta.customer_bought_products_odp.bought_products_v2
    where true
        and p_creation_date in (select calendar_date from calendar_dates)
    group by 1
)

select 
    *
from bought_products_v2