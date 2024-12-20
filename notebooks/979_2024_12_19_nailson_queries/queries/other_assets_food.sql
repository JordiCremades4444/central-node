with calendar_dates as (
    select  
        calendar_date
    from unnest(sequence(date('{{start_date}}'), date('{{end_date}}'), interval '1' day)) as cte (calendar_date)
    where true 
)
, store_impressions as (
select
    dynamic_session_id
    , customer_id
    , case when si.origin in ('CategoryResults', 'Category') and (si.search_id is null or si.search_id = '') then 'StoreWallRanking'
    when (si.origin in ('Widget', 'WidgetResults') and si.category_id is not null) or
    (si.origin in ('FeedCarousel', 'FeedCarouselResults', 'FeedGroup', 'FeedGroupResults')) then 'StoreWallWidget'
    when si.origin in ('HomeWidget', 'HomeWidgetResults', 'Widget', 'WidgetResults') then 'HomeWidget'
    when si.origin in ('HomeSearchBarResults', 'CategoryGroupSearchBarResults') or (si.origin = 'CategoryResults' and si.search_id is not null and search_id <> '') then 'Search'
    else 'Other'
    end as impression_surface
    , count(event_id) as store_impressions
from
    "delta"."customer_behaviour_odp"."enriched_custom_event__store_impression_v3" si
    INNER JOIN calendar_dates 
        on calendar_dates.calendar_date = si.p_creation_date
where True
    and si.store_vertical = 'Food'
    and upper(si.platform) in ('IOS', 'ANDROID')
group by 1,2,3
),
food_bubble_impressions as (
select
    dynamic_session_id
    , customer_id
    , case when origin_event_placement = 'FOOD_SUB_BUBBLES' then 'HomeBubble'
    when origin_event_placement = 'PARTNER_BUBBLES' then 'HomeBubble'
    end as impression_surface
    , count(event_id) as store_impressions
from
    "delta"."central_ads_tech_odp"."campaign_partner_impressions" cpi
    join "delta"."partner_stores_odp"."stores_v2" s
    on s.end_date is null
    and cpi.store_id = s.store_id
    INNER JOIN calendar_dates 
        on calendar_dates.calendar_date = cpi.p_event_date
where True
    and cpi.origin_event_placement in ('FOOD_SUB_BUBBLES', 'PARTNER_BUBBLES')
    and s.store_vertical='Food'
group by 1,2,3
),
all_store_impressions as (
select
    *
from
    store_impressions
union all
select
    *
from
    food_bubble_impressions
),
store_accesses as (
select
    dynamic_session_id
    , customer_id
    , case when origin in ('CategoryResults', 'Category') and (search_id is null or search_id = '') then 'StoreWallRanking'
    when (origin in ('Widget', 'WidgetResults') and category_id is not null) or
    (origin in ('FeedCarousel', 'FeedCarouselResults', 'FeedGroup', 'FeedGroupResults')) then 'StoreWallWidget'
    when origin in ('HomeWidget', 'HomeWidgetResults', 'Widget', 'WidgetResults') then 'HomeWidget'
    when origin in ('HomeSearchBarResults', 'CategoryGroupSearchBarResults') or (origin = 'CategoryResults' and search_id is not null and search_id <> '') then 'Search'
    when origin = 'CategoryGroupBubble' then 'HomeBubble'
    when origin = 'HomeBubble' then 'HomeBubble'
    else 'Other'
    end as impression_surface
    , count(event_id) as store_accesses
from
    "delta"."customer_behaviour_odp"."enriched_custom_event__store_accessed_v3" ac
INNER JOIN calendar_dates 
    on calendar_dates.calendar_date = ac.p_creation_date
where True
    and store_vertical='Food'
group by 1,2,3
),
orders as (
select
    dynamic_session_id
    , customer_id
    , case when base_order_id is not null then 'My profile --> Reorder'
    when origin in ('CategoryGroupBubble') then 'HomeBubble'
    when origin in ('HomeBubble') then 'HomeBubble'
    when origin in ('CategoryResults', 'Category') and (search_id is null or search_id = '') then 'StoreWallRanking'
    when (origin in ('Widget', 'WidgetResults') and category_id is not null) or
    (origin in ('FeedCarousel', 'FeedCarouselResults', 'FeedGroup', 'FeedGroupResults')) then 'StoreWallWidget'
    when origin in ('HomeWidget', 'HomeWidgetResults', 'Widget', 'WidgetResults') then 'HomeWidget'
    when operation_type = 'Search' or origin in ('HomeSearchBarResults', 'CategoryGroupSearchBarResults') then 'Search'
    else 'Other' end
    as impression_surface
    , count(order_id) as orders
from
    "delta"."customer__order_attribution__odp"."order_attribution" att
    INNER JOIN calendar_dates 
        on calendar_dates.calendar_date = att.p_creation_date
where True
    and order_vertical='Food'
group by 1,2,3
)
select
    impression_surface
    , count(store_impressions) impression_sessions
    , count(store_accesses) store_access_sessions
    , count(orders) orders_sessions
from
    orders o
    left join all_store_impressions asi
    using (dynamic_session_id, customer_id, impression_surface)
    left join store_accesses sa
    using (dynamic_session_id, customer_id, impression_surface)
   
where 1=1
AND impression_surface IN ('StoreWallRanking', 'Search', 'HomeBubble', 'My profile --> Reorder', 'Other')
group by 1
