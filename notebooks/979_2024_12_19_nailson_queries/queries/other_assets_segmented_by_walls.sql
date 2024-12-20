with calendar_dates as (
    select  
        calendar_date
    from unnest(sequence(date({start_date}), date({end_date}), interval '1' day)) as cte (calendar_date)
    where true 
)

,stores as (
    select distinct
        sa.store_address_id,
        sa.store_id,
        case 
            when store_subvertical3 in ('Food - Other', 'Food - Food') then 'Food'
            when store_subvertical3 in ('Smoking') then 'Smoking'
            when store_subvertical3 in ('Groceries') then 'Groceries'
            when store_subvertical3 in ('Health') then 'Health'
            when store_subvertical3 in ('Shops') then 'Shops'
        else 'Undefined' end as wall
    from delta.partner_stores_odp.store_addresses_v2 sa
    left join delta.partner_stores_odp.stores_v2 s
        on sa.store_id = s.store_id
    where true
        and store_subvertical3 in ('Smoking','Groceries','Health','Shops', 'Food - Other', 'Food - Food')
        and sa.p_end_date is null -- we take the current picture 
        and s.p_end_date is null -- we take the current picture
)

-- =====================================
-- Store impressions
-- =====================================

,store_impressions as (
    select distinct
        dynamic_session_id,
        case 
            when si.origin in ('CategoryResults', 'Category') and (si.search_id is null or si.search_id = '') then 'StoreWallRanking'
            when si.origin in ('HomeSearchBarResults', 'CategoryGroupSearchBarResults') or (si.origin = 'CategoryResults' and si.search_id is not null and search_id <> '') then 'Search'
        end as impression_surface,
        wall
    from delta.customer_behaviour_odp.enriched_custom_event__store_impression_v3 si
    inner join stores using (store_address_id)
    where True
        and si.p_creation_date in (select calendar_date from calendar_dates)
        and si.origin in ('CategoryResults', 'Category', 'HomeSearchBarResults', 'CategoryGroupSearchBarResults')
)

,bubble_impressions as (
    select distinct
        dynamic_session_id,
        'HomeBubble' as impression_surface,
        wall
    from delta.central_ads_tech_odp.campaign_partner_impressions cpi
    inner join stores
        on stores.store_address_id = cpi.store_address_id
    where True
        and cpi.origin_event_placement in ('FOOD_SUB_BUBBLES', 'PARTNER_BUBBLES')
        and cpi.p_event_date in (select calendar_date from calendar_dates)
)

,all_store_impressions as (
    select
        *
    from store_impressions
    union all
    select
        *
    from bubble_impressions
)

-- =====================================
-- Ennd Store impression
-- =====================================

,store_accesses as (
    select distinct
        dynamic_session_id,
        case 
            when origin in ('CategoryResults', 'Category') and (search_id is null or search_id = '') then 'StoreWallRanking'
            when origin in ('HomeSearchBarResults', 'CategoryGroupSearchBarResults') or (origin = 'CategoryResults' and search_id is not null and search_id <> '') then 'Search'
            when origin in ('CategoryGroupBubble','HomeBubble') then 'HomeBubble'
        end as impression_surface,
        wall
    from delta.customer_behaviour_odp.enriched_custom_event__store_accessed_v3 sa
    inner join stores
        on stores.store_address_id = sa.store_address_id
    where True
        and sa.p_creation_date in (select calendar_date from calendar_dates)    
        and origin in ('CategoryResults', 'Category', 'HomeSearchBarResults', 'CategoryGroupSearchBarResults', 'CategoryGroupBubble', 'HomeBubble')
)

,orders as (
    select distinct
        dynamic_session_id,
        case 
            when base_order_id is not null then 'My profile --> Reorder'
            when origin in ('CategoryGroupBubble', 'HomeBubble') then 'HomeBubble'
            when origin in ('CategoryResults', 'Category') and (search_id is null or search_id = '') then 'StoreWallRanking'
            when operation_type = 'Search' or origin in ('HomeSearchBarResults', 'CategoryGroupSearchBarResults') then 'Search'
        end as impression_surface,
        wall
    from delta.customer__order_attribution__odp.order_attribution o
    inner join stores
        on stores.store_address_id = o.store_address_id
    where True
        and o.p_creation_date in (select calendar_date from calendar_dates)
)

,funnel_store_to_orders as (
    select
        asi.impression_surface,
        asi.wall,
        count(distinct asi.dynamic_session_id) as store_impressions,
        count(distinct sa.dynamic_session_id) as store_accesses,
        count(distinct o.dynamic_session_id) as orders
    from all_store_impressions asi
    left join store_accesses sa 
        on asi.dynamic_session_id = sa.dynamic_session_id
        and asi.impression_surface = sa.impression_surface
        and asi.wall = sa.wall
    left join orders o 
        on asi.dynamic_session_id = o.dynamic_session_id
        and asi.impression_surface = o.impression_surface
        and asi.wall = o.wall
    where True
    group by 1,2
    order by 1,2
)

,funnel_orders_to_store as (
    select
        impression_surface,
        wall,
        0 as store_impressions,
        0 as store_accesses,
        count(distinct o.dynamic_session_id) as orders
    from orders o
    where True
        and impression_surface in ('My profile --> Reorder')
    group by 1,2,3,4
)

,all_funnels as (
    select * from funnel_store_to_orders
    union all
    select * from funnel_orders_to_store
    where True
)

select 
    *
from all_funnels 
where  True
    and impression_surface is not null