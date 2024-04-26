--1) CTEs and list of dates

--1.1 How to create a calendar
with calendar_dates as (select 
        calendar_date
    from unnest(sequence(date_add('day',-7,current_date), date_add('day',-1,current_Date), interval '1' day)) as dates (calendar_date)
)

--1.2 How does unnest work
select * from unnest(sequence(date_add('day',-7,current_date), date_add('day',-1,current_Date), interval '1' day))

--1.3 Use case with calendar date
with calendar_dates as (select 
        calendar_date
    from unnest(sequence(date_add('day',-7,current_date), date_add('day',-1,current_Date), interval '1' day)) as dates (calendar_date)
)

,orders_with_pna as (select
        order_id
    from delta.central__pna_orders__odp.pna_orders_info_v2
    where 1=1 
    and order_is_pna
    and p_creation_date in (select calendar_date from calendar_dates) 
)

,orders as (select
    count(distinct order_id)
    from delta.central_order_descriptors_odp.order_descriptors_v2
    where 1=1 
        and order_id in (select order_id from orders_with_pna)
)

select * from orders

select 1.0000*220469/4636425

--2.1 Explore dDPs
show schemas from delta like '%mfc%'
show schemas from hive like '%mfc%'
describe delta.central_order_descriptors_odp.order_descriptors_v2
--link https://datahub.g8s-data-platform-prod.glovoint.com/

--2.2 Explore possiblities
show functions
--link https://trino.io/docs/current/functions/datetime.html

-- 3 Go vertical

....
customer_xxx       order_X1       BCN       25euros       
customer_xxx       order_X2       BCN       10euros
customer_xxx       order_X3       BCN       20euros
customer_yyy       order_Y1       BCN       10euros
customer_yyy       order_Y2       BCN       50euros
customer_zzz       order_Z1       MAD       5euros
customer_zzz       order_Z2       MAD       5euros
....

-- A)How much does each order represent to the total city? (partition by city)
-- B)Ranking of orders (partition by all)
-- C)Difference compared to last order (partition by customer)
-- D)Is customer retained? (partition by customer)
-- E)Rolling sum of current and previous rows (partition by customer)

-- 3.1 We can make easy horizontal transformations
    select 
        p_creation_date,
        order_id,
        customer_id,
        order_vertical,
        order_city_code,
        order_final_status,
        order_total_purchase_eur
        --order_total_purchase_eur*0.5 as horizontal_transformation
    from delta.central_order_descriptors_odp.order_descriptors_v2
    where 1=1
        and p_creation_date >= date('2023-01-01')
        and customer_id in (139988467,140206914,140249885)

-- 3.2 How much does each order represent to the total city? (partition by city)
    with orders as (select 
            p_creation_date,
            order_id,
            customer_id,
            order_vertical,
            order_city_code,
            order_final_status,
            order_total_purchase_eur
            --order_total_purchase_eur*0.5 as horizontal_transformation
        from delta.central_order_descriptors_odp.order_descriptors_v2
        where 1=1
            and p_creation_date >= date('2023-01-01')
            and customer_id in (139988467,140206914,140249885)
    )

    ,ordres_enriched as (select
            *
            ,sum(case when order_final_status='DeliveredStatus' then order_total_purchase_eur end) over(partition by order_city_code) as total_city_order_total_purchase_eur
        from orders
    )

    select 
        *
        ,round(100.00*order_total_purchase_eur/total_city_order_total_purchase_eur,2) as perc_to_total_city
    from ordres_enriched 
    order by 3,1

-- 3.3 Orders ranking splitted by vertical
    with orders as (select 
            p_creation_date,
            order_id,
            customer_id,
            order_vertical,
            order_city_code,
            order_final_status,
            order_total_purchase_eur
            --order_total_purchase_eur*0.5 as horizontal_transformation
        from delta.central_order_descriptors_odp.order_descriptors_v2
        where 1=1
            and p_creation_date >= date('2023-01-01')
            and customer_id in (139988467,140206914,140249885)
    )

    ,ordres_enriched as (select
            *
            ,rank() over(partition by order_vertical order by order_total_purchase_eur desc) as rank
            ,dense_rank() over(partition by order_vertical order by order_total_purchase_eur desc) as dense_rank
            ,row_number() over(partition by order_vertical order by order_total_purchase_eur desc) as row_number
        from orders
    )

    select 
        *
    from ordres_enriched 

-- 3.4 Difference from previous customer order
    with orders as (select 
            p_creation_date,
            order_id,
            customer_id,
            order_vertical,
            order_city_code,
            order_final_status,
            order_total_purchase_eur
            --order_total_purchase_eur*0.5 as horizontal_transformation
        from delta.central_order_descriptors_odp.order_descriptors_v2
        where 1=1
            and p_creation_date >= date('2023-01-01')
            and customer_id in (139988467,140206914,140249885)
    )

    ,ordres_enriched as (select
            *
            ,coalesce(lag(order_total_purchase_eur) over(partition by customer_id order by p_creation_date asc),0) as previous_order_order_total_purchase_eur
        from orders
    )

    select 
        *,
        order_total_purchase_eur - previous_order_order_total_purchase_eur as diff
    from ordres_enriched 
    where customer_id = 140206914

-- 3.5 Is customer 1 week retained
    with orders as (select 
            p_creation_date,
            order_id,
            customer_id,
            order_vertical,
            order_city_code,
            order_final_status,
            order_total_purchase_eur
            --order_total_purchase_eur*0.5 as horizontal_transformation
        from delta.central_order_descriptors_odp.order_descriptors_v2
        where 1=1
            and p_creation_date >= date('2023-01-01')
            and customer_id in (139988467,140206914,140249885)
    )

    ,ordres_enriched as (select
            *
            ,lead(p_creation_date) over(partition by customer_id order by p_creation_date asc) as next_order
        from orders
    )

    select 
        *,
        if(date_diff('day',p_creation_date,next_order)<=7,true,false)
    from ordres_enriched 
    where customer_id = 140206914
-- 3.6 Rolling sum
    with orders as (select 
                p_creation_date,
                order_id,
                customer_id,
                order_vertical,
                order_city_code,
                order_final_status,
                order_total_purchase_eur
                --order_total_purchase_eur*0.5 as horizontal_transformation
            from delta.central_order_descriptors_odp.order_descriptors_v2
            where 1=1
                and p_creation_date >= date('2023-01-01')
                and customer_id in (139988467,140206914,140249885)
    )

    ,ordres_enriched as (select
            *
            ,sum(order_total_purchase_eur) over(partition by customer_id order by p_creation_date asc rows between 3 preceding and 1 preceding) as rolling_avg
        from orders
    )

    select 
        *
    from ordres_enriched 
    where customer_id = 140206914
    order by p_creation_date asc