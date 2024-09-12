select 
    date(creation_time)
    ,count(distinct id) as count_order
from orders
where true
    and creation_time >= '{day_1} 00:00:00'
    and creation_time < '{day_2} 00:00:00'
group by 1