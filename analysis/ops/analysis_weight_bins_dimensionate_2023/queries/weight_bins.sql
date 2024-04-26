--v1
with Recontacts_cr as (select
        ekc.contact_id,
        count(distinct case when recontacts.contact_id is not null then ekc.contact_id end) as recontacts
        from delta.contact__contact_details__odp.contact_details as ekc
        left join delta.contact_kustomer_odp.dim_kustomer_conversations as detail_ekc 
            on detail_ekc.kustomer_conversation_id=ekc.kustomer_conversation_id
        left join delta.contact__contact_details__odp.contact_details as recontacts 
            on recontacts.kustomer_conversation_glovo_courier_id=ekc.kustomer_conversation_glovo_courier_id  
            and recontacts.contact_id<>ekc.contact_id 
            and recontacts.created_at>ekc.created_at
            and (recontacts.order_id=ekc.order_id or (ekc.order_id is null and recontacts.created_at < (detail_ekc.kustomer_conversation_ended_at + INTERVAL '10' Minute)))
            and recontacts.contact_reason_tree=ekc.contact_reason_tree and ekc.created_AT>DATE('2023-01-01')
            and ekc.contact_reason_tree in ('couriers_1.ongoing.issues_before_at_pu.big_order5')
            and ekc.user_type in ('courier')
    group by 1
)

,raw_data_remake_split as (select
        new_order_id as new_order_id,
        orl.order_courier_total_compensation_eur_currency as extra_order_total_cost_eur,
        odv2.order_parent_relationship_type as relation_type,
        original_order_id as original_order_id,
        count(distinct case when contact_type in ('partial','agent') and  channel not in ('note') then ekc.contact_id end ) as agent_contacts,
        count(distinct case when contact_type in ('partial','agent') and  channel not in ('note') and contact_reason_tree in ('couriers_1.ongoing.issues_before_at_pu.big_order5') then ekc.contact_id end ) as big_order_agent_contacts,
        count(distinct case when contact_type in ('auto-bot') and  channel not in ('note') and contact_reason_tree in ('couriers_1.ongoing.issues_before_at_pu.big_order5') then ekc.contact_id end) extra_big_order_auto_contacts,
        count(distinct case when contact_type in ('auto-bot') and  channel not in ('note') and contact_reason_tree in ('couriers_1.ongoing.issues_before_at_pu.big_order5') then ekc.contact_id end) extra_big_order_recontact_cr,
        count(distinct case when order_courier_big_order_bonus_cost_eur_currency>0 then odv2.order_id end) as total_big_order_promos,
        avg(order_courier_big_order_bonus_cost_eur_currency) as cost_big_order_promo,
        avg(order_courier_base_cost_eur_currency) as base_cost,
        avg(order_courier_distance_cost_eur_currency) AS distance_cost,
        avg(order_courier_waiting_cost_eur_currency) AS wating_time_cost,
        count(distinct case when actor_id=57957168 then new_order_id end) as globot_remake,
        count(distinct case when actor_id not in (57957168) and actor_id is not null and operator_glovo_email is null and action  in ('Remake courier order') then new_order_id end) as LO_Remake,
        count(distinct case when actor_id not in (57957168) and actor_id is not null and operator_glovo_email is not null and action in ('Remake courier order') then new_order_id end)  as RTO_remake,
        avg(case when (actor_id not in (57957168) and actor_id is not null and operator_glovo_email is null and action in ('Remake courier order')) or ( actor_id=57957168) then order_courier_total_compensation_eur_currency else null end ) as LO_Remake_CP0,
        avg(case when actor_id is not null and actor_id=57957168 then order_courier_total_compensation_eur_currency else null end ) as GLOBOT_Remake_CPO,
        avg(case when actor_id is not null and actor_id not in (57957168) and operator_glovo_email is null and action in ('Remake courier order') then order_courier_total_compensation_eur_currency else null end) as AgentLO_Remake_CPO,
        sum(distinct case when channel not in ('note') and contact_reason_tree in ('couriers_1.ongoing.issues_before_at_pu.big_order5') then ekc.csat end) AS GSAT_big_order_extra,
        COUNT(distinct case when channel not in ('note') and contact_reason_tree in ('couriers_1.ongoing.issues_before_at_pu.big_order5') and ((ekc.csat is not null)) then ekc.contact_id else null end) as GSAT_ratings_extra
        --sum(odv2.order_total_purchase_eur) as order_total_purchase_eur
    from delta.central_order_descriptors_odp.order_descriptors_v2 odv2
    left join delta.courier_core_cpo_odp.order_level_v2 orl on orl.order_id=odv2.order_id
    left join delta.contact__order_relations_ddp__odp.order_relations order_relations 
        on order_relations.new_order_id=odv2.order_id and order_relations.order_parent_relationship_type in ('REMAKE','SPLIT')
    left join delta.contact__contact_details__odp.contact_details as EKC 
        on ekc.order_id=order_relations.new_order_id
    left join delta.contact_audit_logs_odp.audit_log_entries as ale 
        on ale.subject_id=cast(odv2.order_id as varchar) 
        and ale.action in ('Remake courier order') 
        and ale.creation_time<=(order_relations.order_created_at+ interval '30' second ) 
        and order_relations.order_created_at<=(ale.creation_time+ interval '1' minute )
    left join delta.central_users_odp.operators as operators on operators.operator_id=ale.actor_id
    where 1=1
        and date(odv2.order_activated_local_at) >date_add('month',-3,date_trunc('month', current_timestamp)) --TO EDIT TAKING 3 MONTHS OF DATA
        and new_order_id is not null
        and order_country_code not in ('UY','TR', 'PR', 'PA', 'EG', 'DO', 'CL', 'CO', 'BR', 'BO', 'AR','EC','PE','CR','GT','HN','FR')
        and order_subvertical in ('MFC','QCPartners')
        and odv2.order_parent_relationship_type in ('REMAKE','SPLIT')
    group by 1,2,3,4
)

,Split_remake_clean_data as (select
        raw_data_remake_split.original_order_id as order_id,
        sum(raw_data_remake_split.extra_order_total_cost_eur) as extra_courier_cost,
    --contacts 63
        sum(agent_contacts) as extra_agent_contacts,
        sum(big_order_agent_contacts) as extra_big_order_agent_contacts,
        sum(extra_big_order_auto_contacts) as extra_big_order_auto_contacts,
        sum(extra_big_order_recontact_cr) as extra_big_order_recontact_cr,
    --promo 69
        sum(total_big_order_promos) as extra_total_big_order_promos,
        sum(cost_big_order_promo) as extrta_cost_big_order_promo,
        count(distinct case when raw_data_remake_split.relation_type='SPLIT' then raw_data_remake_split.new_order_id end) as n_of_splits,
        count(distinct case WHEN raw_data_remake_split.relation_type='REMAKE' then raw_data_remake_split.new_order_id end) as n_of_remakes,
        sum(base_cost) as extra_base_cost,
        --sum (base_compensation) as extra_base_compensation,
        sum(distance_cost) as extra_distance_cost,
        sum(wating_time_cost) as extra_wating_time_cost,
    --Split Costs 80
        sum(distinct case when raw_data_remake_split.relation_type='SPLIT' then (extra_order_total_cost_eur) end) as total_split_cost,
    --Remake Cost
        sum(distinct case when raw_data_remake_split.relation_type='REMAKE' then (extra_order_total_cost_eur) end) as total_Remake_cost,
        sum(globot_remake) as globot_remake,
        sum(LO_remake) as LO_remake,
        sum(RTO_remake) as RTO_remake,
        sum(LO_Remake_CP0) as LO_Remake_CPO,
        sum(GLOBOT_Remake_CPO) as GLOBOT_Remake_CPO,
        sum(AgentLO_Remake_CPO) as AgentLO_Remake_CPO,
    --GSAT
        sum(GSAT_big_order_extra) as GSAT_big_order_extra,
        sum(GSAT_ratings_extra) as GSAT_ratings_extra
    --Total Purchase
        --sum(order_total_purchase_eur) as split_remake_total_purchase_eur -- Splits have 0, but Remakes could have different from zero
    from raw_data_remake_split
    group by 1
)

,Originals as(select
        odv2.order_id as order_id,
        odv2.order_country_code as country_code,
        date_format(date_trunc ('month',order_activated_local_at) ,'%Y-%m-%d') as month,
        date_format(date_trunc ('month',order_activated_local_at) ,'%Y-%m-%d') as month_2,
        date_format(date_trunc ('week',order_activated_local_at) ,'%Y-%m-%d') as week,
        date_format(date_trunc ('week',order_activated_local_at) ,'%Y-%m-%d') as week_number,
        oa.courier_vehicle_type as transport,
        --Weights bins v2
        CASE
            WHEN big_order_verifications.weight_in_grams = 0.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '(-inf,0.0)'
            WHEN big_order_verifications.weight_in_grams > 0.0 AND big_order_verifications.weight_in_grams < 1000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[0.0,1000.0)'
            WHEN big_order_verifications.weight_in_grams >= 1000.0 AND big_order_verifications.weight_in_grams < 2000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[1000.0,2000.0)'
            WHEN big_order_verifications.weight_in_grams >= 2000.0 AND big_order_verifications.weight_in_grams < 3000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[2000.0,3000.0)'
            WHEN big_order_verifications.weight_in_grams >= 3000.0 AND big_order_verifications.weight_in_grams < 4000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[3000.0,4000.0)'
            WHEN big_order_verifications.weight_in_grams >= 4000.0 AND big_order_verifications.weight_in_grams < 5000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[4000.0,5000.0)'
            WHEN big_order_verifications.weight_in_grams >= 5000.0 AND big_order_verifications.weight_in_grams < 6000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[5000.0,6000.0)'
            WHEN big_order_verifications.weight_in_grams >= 6000.0 AND big_order_verifications.weight_in_grams < 7000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[6000.0,7000.0)'
            WHEN big_order_verifications.weight_in_grams >= 7000.0 AND big_order_verifications.weight_in_grams < 8000.0 AND big_order_verifications.big_order_is_missing_data =FALSE  THEN '[7000.0,8000.0)'
            WHEN big_order_verifications.weight_in_grams >= 8000.0 AND big_order_verifications.weight_in_grams < 9000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[8000.0,9000.0)'
            WHEN big_order_verifications.weight_in_grams >= 9000.0 AND big_order_verifications.weight_in_grams < 10000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[9000.0,10000.0)'
            WHEN big_order_verifications.weight_in_grams >= 10000.0 AND big_order_verifications.weight_in_grams < 11000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[10000.0,11000.0)'
            WHEN big_order_verifications.weight_in_grams >= 11000.0 AND big_order_verifications.weight_in_grams < 12000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[11000.0,12000.0)'
            WHEN big_order_verifications.weight_in_grams >= 12000.0 AND big_order_verifications.weight_in_grams < 13000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[12000.0,13000.0)'
            WHEN big_order_verifications.weight_in_grams >= 13000.0 AND big_order_verifications.weight_in_grams < 14000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[13000.0,14000.0)'
            WHEN big_order_verifications.weight_in_grams >= 14000.0 AND big_order_verifications.weight_in_grams < 15000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[14000.0,15000.0)'
            WHEN big_order_verifications.weight_in_grams >= 15000.0 AND big_order_verifications.weight_in_grams < 16000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[15000.0,16000.0)'
            WHEN big_order_verifications.weight_in_grams >= 16000.0 AND big_order_verifications.weight_in_grams < 17000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[16000.0,17000.0)'
            WHEN big_order_verifications.weight_in_grams >= 17000.0 AND big_order_verifications.weight_in_grams < 18000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[17000.0,18000.0)'
            WHEN big_order_verifications.weight_in_grams >= 18000.0 AND big_order_verifications.weight_in_grams < 19000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[18000.0,19000.0)'
            WHEN big_order_verifications.weight_in_grams >= 19000.0 AND big_order_verifications.weight_in_grams < 20000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[19000.0,20000.0)'
            WHEN big_order_verifications.weight_in_grams >= 20000.0 AND big_order_verifications.weight_in_grams < 21000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[20000.0,21000.0)'
            WHEN big_order_verifications.weight_in_grams >= 21000.0 AND big_order_verifications.weight_in_grams < 22000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[21000.0,22000.0)'
            WHEN big_order_verifications.weight_in_grams >= 22000.0 AND big_order_verifications.weight_in_grams < 23000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[22000.0,23000.0)'
            WHEN big_order_verifications.weight_in_grams >= 23000.0 AND big_order_verifications.weight_in_grams < 24000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[23000.0,24000.0)'
            WHEN big_order_verifications.weight_in_grams >= 24000.0 AND big_order_verifications.weight_in_grams < 25000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[24000.0,25000.0)'
            WHEN big_order_verifications.weight_in_grams >= 25000.0 AND big_order_verifications.weight_in_grams < 26000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[25000.0,26000.0)'
            WHEN big_order_verifications.weight_in_grams >= 26000.0 AND big_order_verifications.weight_in_grams < 27000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[26000.0,27000.0)'
            WHEN big_order_verifications.weight_in_grams >= 27000.0 AND big_order_verifications.weight_in_grams < 28000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[27000.0,28000.0)'
            WHEN big_order_verifications.weight_in_grams >= 28000.0 AND big_order_verifications.weight_in_grams < 29000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[28000.0,29000.0)'
            WHEN big_order_verifications.weight_in_grams >= 29000.0 AND big_order_verifications.weight_in_grams < 30000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[29000.0,30000.0)'
            WHEN big_order_verifications.weight_in_grams >= 30000.0 AND big_order_verifications.weight_in_grams < 31000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[30000.0,31000.0)'
            WHEN big_order_verifications.weight_in_grams >= 31000.0 AND big_order_verifications.weight_in_grams < 32000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[31000.0,32000.0)'
            WHEN big_order_verifications.weight_in_grams >= 32000.0 AND big_order_verifications.weight_in_grams < 33000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[32000.0,33000.0)'
            WHEN big_order_verifications.weight_in_grams >= 33000.0 AND big_order_verifications.weight_in_grams < 34000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[33000.0,34000.0)'
            WHEN big_order_verifications.weight_in_grams >= 34000.0 AND big_order_verifications.weight_in_grams < 35000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[34000.0,35000.0)'
            WHEN big_order_verifications.weight_in_grams >= 35000.0 AND big_order_verifications.weight_in_grams < 36000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[35000.0,36000.0)'
            WHEN big_order_verifications.weight_in_grams >= 36000.0 AND big_order_verifications.weight_in_grams < 37000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[36000.0,37000.0)'
            WHEN big_order_verifications.weight_in_grams >= 37000.0 AND big_order_verifications.weight_in_grams < 38000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[37000.0,38000.0)'
            WHEN big_order_verifications.weight_in_grams >= 38000.0 AND big_order_verifications.weight_in_grams < 39000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[38000.0,39000.0)'
            WHEN big_order_verifications.weight_in_grams >= 39000.0 AND big_order_verifications.weight_in_grams < 40000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[39000.0,40000.0)'
            WHEN big_order_verifications.weight_in_grams >= 40000.0 AND big_order_verifications.big_order_is_missing_data =FALSE THEN '[40000.0,inf)'
            ELSE 'Undefined'
        END AS weight_tiers,
    -- Orders 199
        count(distinct case when odv2.order_final_status in ('DeliveredStatus', 'CanceledStatus') then odv2.order_id else null end) as total_orders,
        count(distinct case when odv2.order_final_status in ('CanceledStatus') then odv2.order_id else null end) as canceled_orders,
        count(distinct case when odv2.order_final_status in ('DeliveredStatus') then odv2.order_id else null end) as delivered_orders,
    --Reassigmnents
        count(distinct case when odv2.order_number_of_assignments>1 then odv2.order_id else null end ) as reassigned_orders,
        avg(case when (odv2.order_number_of_assignments>1) then odv2.order_number_of_assignments else null end) as num_of_reassigments,
        count(distinct case when order_requested_reassignment_requested_at>ekc.created_At AND order_requested_reassignment_requested_at<(ekc.created_at+ interval '1' Minute ) then odv2.order_id else null end) as reassigned_orders_afterBO,
    --Promos 212
        count(distinct case when orl.order_courier_big_order_bonus_cost_eur_currency>0 then odv2.order_id end) as original_total_big_order_promos,
        avg(orl.order_courier_big_order_bonus_cost_eur_currency) as original_cost_big_order_promo,
    --Weight
        avg(case when big_order_verifications.big_order_is_missing_data=false then big_order_verifications.weight_in_grams else null end) as weight_in_grams,
    --LiveOps Contacts
        count(distinct case when contact_type IN ('partial','agent') and channel not in ('note')  THEN ekc.contact_id end ) as original_agent_contacts,
        count(distinct case when contact_type IN ('partial','agent') and  channel not in ('note') and contact_reason_tree in ('couriers_1.ongoing.issues_before_at_pu.big_order5') then ekc.contact_id end ) as original_big_order_agent_contacts,
        count(distinct case when contact_type IN ('auto-bot') and channel not in ('note') THEN ekc.contact_id end) original_auto_contacts,
        count(distinct case when contact_type IN ('auto-bot') and  channel not in ('note') and contact_reason_tree in ('couriers_1.ongoing.issues_before_at_pu.big_order5') then ekc.contact_id end) original_big_order_auto_contacts,
        Count(distinct case when rc.recontacts>0 then ekc.contact_id end) as big_order_recontact_cr,
    -- Courier Cost and reveneue
        avg(coalesce((orl.order_courier_total_compensation_eur_currency),0)) as courier_Cost,
        avg(coalesce((order_base_delivery_revenue_local_currency),0)) as total_revenue,
    -- Splits and remakes info 232
        count(distinct case when (n_of_splits>0 or n_of_remakes>0 )then ODV2.order_id else null end) as n_of_orders_with_split_remakes,
        avg(n_of_splits) as n_of_splits,
        avg(n_of_remakes) as n_of_remakes,
        avg(extra_courier_cost) as extra_courier_cost,
        avg(extra_agent_contacts) as extra_agent_contacts,
        avg(extra_big_order_agent_contacts) as extra_big_order_agent_contacts,
        avg(extra_big_order_auto_contacts) as extra_big_order_auto_contacts,
        avg(extra_big_order_recontact_cr) as extra_big_order_recontact_cr,
        avg(extra_total_big_order_promos) as extra_total_big_order_promos,
        avg(extrta_cost_big_order_promo) as extrta_cost_big_order_promo,
    --Compensation
        count(distinct case when total_balance_amount_eur>0 then oif.order_id end) as compensated_orders,
        avg(coalesce(total_balance_amount_eur,0)+coalesce(total_balance_amount_eur,0)) as comp_customer_wo_partner,
        avg(coalesce(refund_assumed_by_partner_amount_eur,0)) as charged_partner,
        avg(case when partner_cancellation_strategy='PAY_PRODUCTS' and customer_cancellation_strategy in ('CHARGE_NOTHING','CHARGE_CANCELLATION') then odv2.order_estimated_purchase_eur else null end )as free_canc,
    -- Bundling
        count(distinct case when num_bundles>0 then odv2.order_id else null end) as bundled_order,
        count(distinct case when is_unbundled=TRUE then odv2.order_id else null end) as unbundled_order,
    --detail_Courier_Cost
        avg(order_courier_base_cost_eur_currency) as base_cost,
        avg(order_courier_base_cost_eur_currency) as base_compensation,
        avg(order_courier_distance_cost_eur_currency) AS distance_cost,
        avg(order_courier_waiting_cost_eur_currency) AS wating_time_cost,
    --Detail_Extra_Courier_cost 266
        avg(extra_base_cost) as extra_base_cost,
        avg(extra_base_cost)as extra_base_compensation,
        avg(extra_distance_cost) as extra_distance_cost ,
        avg(extra_wating_time_cost) as extra_wating_time_cost,
    --Split Cost
        avg(total_split_cost) as total_split_cost ,
    --Remake Cost
        avg(total_remake_cost) as total_remake_cost,
        avg(globot_remake) as globot_remake,
    --gsat
        sum(distinct case when channel not in ('note') and contact_reason_tree in ('couriers_1.ongoing.issues_before_at_pu.big_order5') then ekc.csat end) as GSAT_big_order_original,
        COUNT(DISTINCT CASE WHEN  channel not in ('note') and contact_reason_tree in ('couriers_1.ongoing.issues_before_at_pu.big_order5') and ((ekc.csat is not null)) then ekc.contact_id else null end) as GSAT_ratings,
        sum(GSAT_big_order_extra) as GSAT_big_order_extra,
        sum(GSAT_ratings_extra) as GSAT_ratings_extra
    --Total Purchase Eur
        --sum(split_remake_total_purchase_eur) as split_remake_total_purchase_eur
    from delta.central_order_descriptors_odp.order_descriptors_v2 as odv2
    left join delta.courier_planning_odp.order_activity as OA 
        on oa.order_id=odv2.order_id
    left join delta.courier_core_cpo_odp.order_level_v2 orl 
        on orl.order_id=odv2.order_id
    left join delta.contact__contact_details__odp.contact_details AS EKC 
        on ekc.order_id=odv2.order_id
    left join delta.finance_delivery_and_service_revenue_odp.order_delivery_and_service_revenue as revenue 
        on revenue.order_id=odv2.order_id
    left join delta.courier_delivery_flow_odp.order_requested_reassignments orr 
        on ekc.order_id=orr.order_id and ekc.kustomer_conversation_glovo_courier_id=orr.courier_id 
        AND order_requested_reassignment_requested_at>ekc.created_At 
        AND order_requested_reassignment_requested_at<(ekc.created_at+ interval '1' Minute)
    left join delta.courier_order_management_odp.bundled_order_details as bundle 
        on bundle.order_id=odv2.order_id
    left join delta.contact_order_incident_facts_odp.fct_order_incidents oif 
        on oif.order_id=odv2.order_id
    left join recontacts_cr rc 
        on rc.contact_id=ekc.contact_id
    left join Split_remake_clean_data 
        on Split_remake_clean_data.order_id = odv2.order_id
    Left join delta.central_big_order_verifications_odp.big_order_verifications as big_order_verifications 
        on big_order_verifications.order_id=odv2.order_id
    left join delta.contact__order_relations_ddp__odp.order_relations  order_relations 
        on order_relations.new_order_id=odv2.order_id  and order_relations.order_parent_relationship_type in ('REMAKE','SPLIT')
    where 1=1 
        and date(odv2.order_activated_local_at)>date_add('month',-3,date_trunc('month',current_timestamp))
        and new_order_id is null
        and order_subvertical in ('QCPartners','MFC')
    group by 1,2,3,4,5,6,7,8
)

select
    country_code as country_code,
    transport as transport,
    weight_tiers as weight_tiers,
    sum(total_orders) as total_orders,
    sum(delivered_orders) as delivered_orders,
    sum(canceled_orders) as canceled_orders,
    -- Reassigments
    sum(reassigned_orders) as reassigned_orders ,
    -- Promos 316
    sum(original_total_big_order_promos) as original_total_big_order_promos ,
    sum(original_cost_big_order_promo) as original_cost_big_order_promo,
    --LiveOps contacts
    sum(original_agent_contacts) as original_agent_contacts,
    sum(original_big_order_agent_contacts) as original_big_order_agent_contacts,
    -- Courier cost and revenue
    sum(courier_Cost) as courier_cost,
    sum(total_revenue) as total_revenue,
    -- Splits and remakes info
    sum(n_of_splits) as n_of_splits,
    sum(n_of_remakes) as n_of_remakes,
    sum(extra_courier_cost) as extra_courier_cost,
    sum(extra_agent_contacts) as extra_agent_contacts,
    sum(extra_big_order_agent_contacts) as extra_big_order_agent_contacts,
    sum(extra_total_big_order_promos) as extra_total_big_order_promos,
    sum(extrta_cost_big_order_promo) as extrta_cost_big_order_promo,
    sum(n_of_orders_with_split_remakes) as n_of_orders_with_split_remakes,
    --compensation
    sum(compensated_orders) as compensated_orders,
    sum(comp_customer_wo_partner) as comp_customer_wo_partner,
    sum (charged_partner) as charged_partner,
    sum(free_canc) as free_canc,
    -- Bundle_orders
    sum(bundled_order) as bundled_order,
    sum(unbundled_order) as unbundled_order,
    --Details_Costs_original
    sum(originals.base_cost) as base_cost,
    sum(base_compensation) as base_compensation,
    sum(distance_cost) as distance_cost ,
    sum(wating_time_cost) as wating_time_cost,
    --Details_Costs_Extra
    sum(extra_base_cost) as extra_base_cost,
    sum(extra_base_compensation) as extra_base_compensation,
    sum(extra_distance_cost) as extra_distance_cost ,
    sum (extra_wating_time_cost) as extra_wating_time_cost,
    --Split_cost
    sum (total_split_cost) as total_split_cost,
    --remake_cost
    sum (total_remake_cost) as total_remake_cost,
    --weight
    avg(Weight_in_grams) as Weight_in_the_order,
    --Autocontacts
    sum(original_big_order_auto_contacts) as original_big_order_auto_contacts,
    sum(extra_big_order_auto_contacts) as extra_big_order_auto_contacts,
    --Recontacts
    sum(big_order_recontact_cr) as big_order_recontact_cr,
    sum(extra_big_order_recontact_cr) as extra_big_order_recontact_cr,
    --REASSIGMENTS
    sum(num_of_reassigments) as n_of_Reassigments,
    sum(reassigned_orders_afterBO) AS reassigned_orders_afterBO,
    sum(globot_remake) as globot_remake,
    --GSAT
    sum(GSAT_big_order_original) AS GSAT_big_order_original,
    sum(GSAT_ratings) AS GSAT_ratings,
    sum(GSAT_big_order_extra) as GSAT_big_order_extra,
    sum(GSAT_ratings) as GSAT_ratings_extra
from originals
group by 1,2,3
--endv1


