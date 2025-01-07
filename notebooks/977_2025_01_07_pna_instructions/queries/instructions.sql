with calendar_dates as (
    select 
        calendar_date
    from unnest(sequence(date('{{start_date}}'), date('{{end_date}}'), interval '1' day)) as cte (calendar_date)
    where true 
)

,customers_glovo as (
    select
        u.user_id as customer_id
    from delta.central_users_odp.users_v2 u
    where true
        and not user_is_staff
        and not user_is_glovo_employee
        and user_type = 'Customer'
)

,stores as (
    select distinct
        sa.store_address_id,
        sa.store_id,
        s.store_name
    from delta.partner_stores_odp.store_addresses_v2 sa
    left join delta.partner_stores_odp.stores_v2 s
        on sa.store_id = s.store_id
    left join delta.central_geography_odp.cities_v2 c
        on s.city_code = c.city_code
    where true
        and sa.p_end_date is null
        and s.p_end_date is null
        and s.store_subvertical = 'QCPartners'
        and s.store_subvertical2 = 'Groceries'
        and sa.store_address_id in {{sad_ids}} --eataly and penny partners
)

,basket_screens as (
    select
        cu.creation_date,
        cu.dynamic_session_id,
        cu.event_id,
        cu.customer_id,
        cu.creation_time,
        cu.platform,
        stores.store_name
    from sensitive_delta.customer_mpcustomer_odp.custom_event cu
    inner join calendar_dates cd
        on cd.calendar_date = cu.creation_date
    inner join stores
        on stores.store_address_id = cast(cu.custom_attributes__store_address_id as bigint)
    inner join customers_glovo
        on customers_glovo.customer_id = cu.customer_id
    where true
        and cu.event_name = 'Basket Screen'
)

,cart_interacted as (
    select
        cu.creation_date,
        cu.event_name,
        cu.custom_attributes__cart_interaction_type,
        cu.dynamic_session_id,
        cu.event_id,
        cu.customer_id,
        cu.creation_time
    from sensitive_delta.customer_mpcustomer_odp.custom_event cu
    inner join calendar_dates cd
        on cd.calendar_date = cu.creation_date
    inner join customers_glovo
        on customers_glovo.customer_id = cu.customer_id
    where true
        and cu.event_name = 'Cart Interacted'
        and cu.custom_attributes__cart_interaction_type = 'GoPNAInstructions'
)

,instruction_impression as (
    select
        cu.creation_date,
        cu.event_name,
        cu.custom_attributes__instruction_added,
        cu.dynamic_session_id,
        cu.event_id,
        cu.customer_id,
        cu.creation_time
    from sensitive_delta.customer_mpcustomer_odp.custom_event cu
    inner join calendar_dates cd
        on cd.calendar_date = cu.creation_date
    inner join customers_glovo
        on customers_glovo.customer_id = cu.customer_id
    where true
        and cu.event_name = 'Instruction Impression'
        and cu.custom_attributes__instruction_type = 'ProductInstructionPNA'
)

,instruction_added as (
    select
        cu.creation_date,
        cu.event_name,
        cu.custom_attributes__instruction_added,
        cu.dynamic_session_id,
        cu.event_id,
        cu.customer_id,
        cu.creation_time
    from sensitive_delta.customer_mpcustomer_odp.custom_event cu
    inner join calendar_dates cd
        on cd.calendar_date = cu.creation_date
    inner join customers_glovo
        on customers_glovo.customer_id = cu.customer_id
    where true
        and cu.event_name = 'Instruction Added'
        and cu.custom_attributes__instruction_type = 'ProductInstructionPNA'
)

select
    bs.platform,
    bs.store_name,
    bs.creation_date,
    --basket screen
    count(distinct bs.customer_id) as customers_basket_screen,
    count(distinct bs.dynamic_session_id) as sessions_basket_screen,
    count(distinct bs.event_id) as events_basket_screen,
    --cart interacted
    count(distinct ci.customer_id) as customers_cart_interacted,
    count(distinct ci.dynamic_session_id) as sessions_cart_interacted,
    count(distinct ci.event_id) as events_cart_interacted,
    --instruction impression
    count(distinct ii.customer_id) as customers_instruction_impression,
    count(distinct ii.dynamic_session_id) as sessions_instruction_impression,
    count(distinct ii.event_id) as events_instruction_impression,
        --REMOVE
        count(distinct case when ii.custom_attributes__instruction_added = 'REMOVE' then ii.customer_id else null end) as customers_instruction_impression_remove,
        count(distinct case when ii.custom_attributes__instruction_added = 'REMOVE' then ii.dynamic_session_id else null end) as sessions_instruction_impression_remove,
        count(distinct case when ii.custom_attributes__instruction_added = 'REMOVE' then ii.event_id else null end) as events_instruction_impression_remove,
        --REPLACE
        count(distinct case when ii.custom_attributes__instruction_added = 'REPLACE_WITH_BEST_MATCH' then ii.customer_id else null end) as customers_instruction_impression_replace,
        count(distinct case when ii.custom_attributes__instruction_added = 'REPLACE_WITH_BEST_MATCH' then ii.dynamic_session_id else null end) as sessions_instruction_impression_replace,
        count(distinct case when ii.custom_attributes__instruction_added = 'REPLACE_WITH_BEST_MATCH' then ii.event_id else null end) as events_instruction_impression_replace,
    --instruction added
    count(distinct ia.customer_id) as customers_instruction_added,
    count(distinct ia.dynamic_session_id) as sessions_instruction_added,
    count(distinct ia.event_id) as events_instruction_added,
        --REMOVE
        count(distinct case when ia.custom_attributes__instruction_added = 'REMOVE' then ia.customer_id else null end) as customers_instruction_added_remove,
        count(distinct case when ia.custom_attributes__instruction_added = 'REMOVE' then ia.dynamic_session_id else null end) as sessions_instruction_added_remove,
        count(distinct case when ia.custom_attributes__instruction_added = 'REMOVE' then ia.event_id else null end) as events_instruction_added_remove,
        --REPLACE
        count(distinct case when ia.custom_attributes__instruction_added = 'REPLACE_WITH_BEST_MATCH' then ia.customer_id else null end) as customers_instruction_added_replace,
        count(distinct case when ia.custom_attributes__instruction_added = 'REPLACE_WITH_BEST_MATCH' then ia.dynamic_session_id else null end) as sessions_instruction_added_replace,
        count(distinct case when ia.custom_attributes__instruction_added = 'REPLACE_WITH_BEST_MATCH' then ia.event_id else null end) as events_instruction_added_replace
from basket_screens bs
left join cart_interacted ci
    on bs.dynamic_session_id = ci.dynamic_session_id
    and bs.creation_time < ci.creation_time
left join instruction_impression ii
    on bs.dynamic_session_id = ii.dynamic_session_id
    and bs.creation_time < ii.creation_time
left join instruction_added ia
    on bs.dynamic_session_id = ia.dynamic_session_id
    and bs.creation_time < ia.creation_time
group by 1,2,3
order by 1,2,3
