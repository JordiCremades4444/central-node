with calendar_dates as (select
        calendar_date
    from unnest(sequence(
        date_add('day',-{days_in_advance},current_date),-- initial date
        date_add('day',-1,current_date),-- final date
        interval '1' day-- step
    )) as dates (calendar_date)
    where 1=1
)

,spm_ddp_info as (
    select 
        p_creation_date
        ,count(*) count_order_product_id_partner_spm_ddp_info
    from delta.central__spm_ddp__odp.spm_ddp_info
    where true 
        and p_creation_date in (select calendar_date from calendar_dates)
    group by 1
)

select 
    *
from spm_ddp_info