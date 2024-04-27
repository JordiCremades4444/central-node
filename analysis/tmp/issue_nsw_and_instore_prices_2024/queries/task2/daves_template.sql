WITH glovo_test_groups AS (
    /*
    Getting the list of Customer ID that are control or variant in the implementation of the New Store Wall for Groceries. 
    Between 2024-02-09 and 2024-02-16 it's discarded because it was the time of Valentine's Day and we had all the widgets/touchpoints to use the same salt and show retail stores.
    */
    SELECT DISTINCT data__experimentation_allocation_key AS customer_id,
        CASE WHEN data__experimentation_variant_value = 'Control Group' THEN 'Control SW'
            WHEN data__experimentation_variant_value = Variant' 1' THEN 'Variant SW'
            ELSE 'Out of Experiment' END AS exp_group_nsw
    FROM  "delta"."mlp_feature_store_experiment_exposure_odp"."mlp_experiment_exposure"
    WHERE p_event_date >  DATE '2023-10-10' AND NOT (p_event_date >= date '2024-02-09' AND p_event_date <= date '2024-02-16') --we discard st.valentine switch of users
        AND data__experimentation_toggle_id = 'ZAP_CATEGORY_LANDING_PAGE'
        AND data__experimentation_variant_value IN ('Variant 1', 'Control Group')
), top_brands_instore_groups AS (
    SELECT DISTINCT data__experimentation_allocation_key AS customer_id,
        CASE WHEN data__experimentation_variant_value = 'Control Group' THEN 'Control'
            WHEN data__experimentation_variant_value = 'InStorePrices' THEN 'Variant'
            ELSE 'Out of Experiment' END AS exp_group_instore -- as experiment_group, data__experimentation_toggle_id
    FROM  "delta"."mlp_feature_store_experiment_exposure_odp"."mlp_experiment_exposure"
    WHERE p_event_date > DATE '2024-01-18' AND NOT (p_event_date >= date '2024-02-09' AND p_event_date <= date '2024-02-16') 
        AND data__experimentation_toggle_id = 'ZAP_NSW_EXPERIMENT'
        AND data__experimentation_variant_value IN ('InStorePrices', 'Control Group')
), gnc_migration AS (
    SELECT customer_id
        , order_created_local_at
    FROM "delta"."central__qcommerce_first_orders__odp"."groceries_first_orders_info"
), category_events AS (
    SELECT p_creation_date
        , dynamic_session_id
        , event_id
        , customer_id
        , country
        , creation_time_local
        , creation_time
        , platform
        , category_id
    FROM delta.customer_behaviour_odp.enriched_custom_event__category_opened_v3
    WHERE p_creation_date >=  DATE '2023-09-01' {INC_P_CR_4D} AND category_id IN (4, 540, 622, 679, 682, 762, 875, 1015, 1082, 1195, 1197, 1214, 1314, 1316, 1551, 1718)
), store_walls_events AS (
    SELECT p_creation_date
        , dynamic_session_id
        , event_id
        , country
        , creation_time
        , customer_id
        , category_id
        , platform
    FROM delta.customer_behaviour_odp.enriched_screen_view__stores_v3
    WHERE  p_creation_date >=  DATE '2023-09-01'   {INC_P_CR_4D} AND category_id IN (4, 540, 622, 679, 682, 762, 875, 1015, 1082, 1195, 1197, 1214, 1314, 1316, 1551, 1718)
    /*THS SECTION IS EXCLUSIVELY TO RECOVER STORE WALL EVENTS THAT WENT TO THE WRONG PLACE */
    UNION ALL
    SELECT 
       creation_date AS p_creation_date
       , dynamic_session_id
       , cast(event_id as bigint) AS event_id
       , custom_attributes__country AS country
       , creation_time
       , customer_id
       , cast(custom_attributes__category_id as bigint) AS category_id
       , device_info__platform AS platform
    FROM 
        "legacy_delta"."refinery_mpcustomer"."custom_event"  --"sensitive_delta"."customer_mpcustomer_odp"."custom_event" ev
    where 
       creation_date >= DATE('2023-09-21') {INC_P_CR_4D.replace("p_creation_date", "creation_date")}
        and event_name = 'Stores'
        AND cast(custom_attributes__category_id as bigint) IN (4, 540, 622, 679, 682, 762, 875, 1015, 1082, 1195, 1197, 1214, 1314, 1316, 1551, 1718) 
/* As we are not showing it, we discard it for the moment.       
), prime_banner_shown AS (
        SELECT p_creation_date
        , dynamic_session_id
        , event_id
        , creation_time_local
        , creation_time
        , customer_id
        , country
        , platform
    FROM delta.customer_behaviour_odp.enriched_custom_event__prime_banner_shown_v3
    WHERE  p_creation_date  >=  DATE '2023-09-01'  {INC_P_CR_4D}
        AND banner_id IN ('STORE_WALL','STORE_WALL_BANNER_V2','STORE_WALL_V1','STORE_WALL_FILTER_BANNER_V1','STORE_WALL_FILTER')
), prime_banner_tapped AS (
        SELECT p_creation_date
        , dynamic_session_id
        , event_id
        , creation_time_local
        , creation_time
        , customer_id
        , country
         , platform
    FROM delta.customer_behaviour_odp.enriched_custom_event__prime_banner_tapped_v3
    WHERE  p_creation_date >=  DATE '2023-09-01' {INC_P_CR_4D}
        AND banner_id IN ('STORE_WALL','STORE_WALL_BANNER_V2','STORE_WALL_V1','STORE_WALL_FILTER_BANNER_V1','STORE_WALL_FILTER')
*/
), store_events AS (
    SELECT p_creation_date
        , dynamic_session_id
        , event_id
        , customer_id
        , stores.creation_time
        , stores.creation_time_local
        , stores.store_id
        , store_address_id
        , stores_v2.store_sub_business_unit
        , stores.country
        , platform
        , stores_v2.store_subvertical
        , CASE WHEN (stores_v2.store_subvertical <> 'MFC'
               AND stores_v2.store_subvertical3 = 'Groceries'
               AND UPPER(stores_v2.store_sub_business_unit) NOT IN (
                    'CONVENIENCE', 'SUPERMARKET', 'OTHER'
                    )
                  )
               THEN True ELSE False END AS is_specialties
    FROM delta.customer_behaviour_odp.enriched_screen_view__store_v3 AS stores
    INNER JOIN delta.partner_stores_odp.stores_v2
        ON stores_v2.p_end_date IS NULL
        AND stores_v2.store_subvertical2 = 'Groceries'
        AND stores_v2.store_id = stores.store_id
    WHERE  p_creation_date >= DATE '2023-09-01' {INC_P_CR_4D} 
), checkout_events AS (
    SELECT p_creation_date
        , store_id
        , event_id
        , dynamic_session_id
        , creation_time_local
        , creation_time
        , customer_id
        , store_address_id
        , country
        , platform
    FROM  delta.customer_behaviour_odp.enriched_screen_view__checkout_template_received_v3
    WHERE  p_creation_date >= DATE '2023-09-01'  {INC_P_CR_4D} 
), order_events AS (
    SELECT p_creation_date
        , store_id
        , event_id
        , dynamic_session_id
        , creation_time_local
        , creation_time
        , customer_id
        , store_address_id
        , country
        , platform
    FROM  delta.customer_behaviour_odp.enriched_backend_event__checkout_order_created_v3
    WHERE  p_creation_date >= DATE '2023-09-01' {INC_P_CR_4D} 
), all_funnel AS (
    SELECT
        ce.p_creation_date
        , UPPER(COALESCE(ce.country, swe.country, se.country, oe.country, che.country)) AS country
        , UPPER(COALESCE(ce.platform, swe.platform, se.platform, oe.platform, che.platform)) AS platform
        , COALESCE(exp_group_nsw, 'Out Of Experiment SW') AS experiment_group_nsw
        , COALESCE(exp_group_instore, 'Out Of Experiment') AS experiment_group_instore
        , CASE WHEN ce.creation_time_local >= order_created_local_at THEN 'GRC'
            WHEN gnc_migration.customer_id IS NULL OR ce.creation_time_local < order_created_local_at THEN 'GNC'
            ELSE 'Customer ID NULL' END AS grocery_user_profile

        , COUNT(DISTINCT(ce.event_id)) AS count_events_sprmkt
        , COUNT(DISTINCT(swe.event_id)) AS count_events_store_wall_after_bubble
        /*
        , COUNT(DISTINCT(pbs.event_id)) AS count_events_prime_banner_shown_after_bubble
        , COUNT(DISTINCT(pbt.event_id)) AS count_events_prime_banner_tap_after_bubble
        */
        , COUNT(DISTINCT(se.event_id)) AS count_events_store_after_bubble
        , COUNT(DISTINCT(oe.event_id)) AS count_events_orders_after_bubble
        , COUNT(DISTINCT(che.event_id)) AS count_events_checkout_after_bubble

        , COUNT(DISTINCT(ce.customer_id)) AS count_customers_sprmkt
        , COUNT(DISTINCT(swe.customer_id)) AS count_customers_store_wall_after_bubble
        /*
        , COUNT(DISTINCT(pbs.customer_id)) AS count_customers_prime_banner_shown_after_bubble
        , COUNT(DISTINCT(pbt.customer_id)) AS count_customers_prime_banner_tap_after_bubble
        */
        , COUNT(DISTINCT(se.customer_id)) AS count_customers_store_after_bubble
        , COUNT(DISTINCT(oe.customer_id)) AS count_customers_orders_after_bubble
        , COUNT(DISTINCT(che.customer_id)) AS count_customers_checkout_after_bubble

        , COUNT(DISTINCT(ce.dynamic_session_id)) AS count_sessions_sprmkt
        , COUNT(DISTINCT(swe.dynamic_session_id)) AS count_sessions_store_wall_after_bubble
        /*
        , COUNT(DISTINCT(pbs.dynamic_session_id)) AS count_sessions_prime_banner_shown_after_bubble
        , COUNT(DISTINCT(pbt.dynamic_session_id)) AS count_sessions_prime_banner_tap_after_bubble
        */
        , COUNT(DISTINCT(se.dynamic_session_id)) AS count_sessions_store_after_bubble
        , COUNT(DISTINCT(oe.dynamic_session_id)) AS count_sessions_orders_after_bubble
        , COUNT(DISTINCT(che.dynamic_session_id)) AS count_sessions_checkout_after_bubble

        , COUNT(DISTINCT(CASE WHEN se.is_specialties THEN se.dynamic_session_id END)) AS count_sessions_store_specialties_after_bubble
        , COUNT(DISTINCT(CASE WHEN se.is_specialties THEN oe.dynamic_session_id END)) AS count_sessions_orders_specialties_after_bubble
        , COUNT(DISTINCT(CASE WHEN se.is_specialties THEN che.dynamic_session_id END)) AS count_sessions_checkout_specialties_after_bubble
        , COUNT(DISTINCT(CASE WHEN se.is_specialties THEN se.customer_id END)) AS count_customers_store_specialties_after_bubble
        , COUNT(DISTINCT(CASE WHEN se.is_specialties THEN oe.customer_id END)) AS count_customers_orders_specialties_after_bubble
        , COUNT(DISTINCT(CASE WHEN se.is_specialties THEN che.customer_id END)) AS count_customers_checkout_specialties_after_bubble
        , COUNT(DISTINCT(CASE WHEN se.is_specialties THEN se.event_id END)) AS count_events_store_specialties_after_bubble
        , COUNT(DISTINCT(CASE WHEN se.is_specialties THEN oe.event_id END)) AS count_events_orders_specialties_after_bubble
        , COUNT(DISTINCT(CASE WHEN se.is_specialties THEN che.event_id END)) AS count_events_checkout_specialties_after_bubble
        
        , COUNT(DISTINCT(CASE WHEN store_subvertical = 'MFC' THEN se.dynamic_session_id END)) AS count_sessions_store_mfc_after_bubble
        , COUNT(DISTINCT(CASE WHEN store_subvertical = 'MFC' THEN oe.dynamic_session_id END)) AS count_sessions_orders_mfc_after_bubble
        , COUNT(DISTINCT(CASE WHEN store_subvertical = 'MFC' THEN che.dynamic_session_id END)) AS count_sessions_checkout_mfc_after_bubble
        , COUNT(DISTINCT(CASE WHEN store_subvertical = 'MFC' THEN se.customer_id END)) AS count_customers_store_mfc_after_bubble
        , COUNT(DISTINCT(CASE WHEN store_subvertical = 'MFC' THEN oe.customer_id END)) AS count_customers_orders_mfc_after_bubble
        , COUNT(DISTINCT(CASE WHEN store_subvertical = 'MFC' THEN che.customer_id END)) AS count_customers_checkout_mfc_after_bubble
        , COUNT(DISTINCT(CASE WHEN store_subvertical = 'MFC' THEN oe.event_id END)) AS count_events_orders_mfc_after_bubble
        , COUNT(DISTINCT(CASE WHEN store_subvertical = 'MFC' THEN che.event_id END)) AS count_events_checkout_mfc_after_bubble
        
        , COUNT(DISTINCT(CASE WHEN store_subvertical != 'MFC' AND UPPER(store_sub_business_unit) IN ('CONVENIENCE', 'SUPERMARKET', 'OTHER') THEN se.dynamic_session_id END)) AS count_sessions_store_grocpart_after_bubble
        , COUNT(DISTINCT(CASE WHEN store_subvertical != 'MFC' AND UPPER(store_sub_business_unit) IN ('CONVENIENCE', 'SUPERMARKET', 'OTHER') THEN oe.dynamic_session_id END)) AS count_sessions_orders_grocpart_after_bubble
        , COUNT(DISTINCT(CASE WHEN store_subvertical != 'MFC' AND UPPER(store_sub_business_unit) IN ('CONVENIENCE', 'SUPERMARKET', 'OTHER') THEN che.dynamic_session_id END)) AS count_sessions_checkout_grocpart_after_bubble
        , COUNT(DISTINCT(CASE WHEN store_subvertical != 'MFC' AND UPPER(store_sub_business_unit) IN ('CONVENIENCE', 'SUPERMARKET', 'OTHER') THEN se.customer_id END)) AS count_customers_store_grocpart_after_bubble
        , COUNT(DISTINCT(CASE WHEN store_subvertical != 'MFC' AND UPPER(store_sub_business_unit) IN ('CONVENIENCE', 'SUPERMARKET', 'OTHER') THEN oe.customer_id END)) AS count_customers_orders_grocpart_after_bubble
        , COUNT(DISTINCT(CASE WHEN store_subvertical != 'MFC' AND UPPER(store_sub_business_unit) IN ('CONVENIENCE', 'SUPERMARKET', 'OTHER') THEN che.customer_id END)) AS count_customers_checkout_grocpart_after_bubble
        , COUNT(DISTINCT(CASE WHEN store_subvertical != 'MFC' AND UPPER(store_sub_business_unit) IN ('CONVENIENCE', 'SUPERMARKET', 'OTHER') THEN se.event_id END)) AS count_events_store_grocpart_after_bubble
        , COUNT(DISTINCT(CASE WHEN store_subvertical != 'MFC' AND UPPER(store_sub_business_unit) IN ('CONVENIENCE', 'SUPERMARKET', 'OTHER') THEN oe.event_id END)) AS count_events_orders_grocpart_after_bubble
        , COUNT(DISTINCT(CASE WHEN store_subvertical != 'MFC' AND UPPER(store_sub_business_unit) IN ('CONVENIENCE', 'SUPERMARKET', 'OTHER') THEN che.event_id END)) AS count_events_checkout_grocpart_after_bubble

    FROM category_events AS ce
    LEFT JOIN store_walls_events AS swe
        ON swe.p_creation_date BETWEEN ce.p_creation_date AND DATE_ADD('day', 2, ce.p_creation_date)
        AND ce.dynamic_session_id = swe.dynamic_session_id
        AND swe.creation_time BETWEEN ce.creation_time AND DATE_ADD('minute', 1, ce.creation_time)
        AND ce.category_id = swe.category_id
    LEFT JOIN store_events AS se
        ON se.p_creation_date BETWEEN ce.p_creation_date AND DATE_ADD('day', 2, ce.p_creation_date)
        AND ce.dynamic_session_id = se.dynamic_session_id
        AND se.creation_time BETWEEN swe.creation_time AND DATE_ADD('minute', 120, swe.creation_time)
/*
    LEFT JOIN prime_banner_shown AS pbs
        ON pbs.p_creation_date BETWEEN ce.p_creation_date AND DATE_ADD('day', 2, ce.p_creation_date)
        AND ce.dynamic_session_id = pbs.dynamic_session_id
        AND pbs.creation_time BETWEEN swe.creation_time AND DATE_ADD('minute', 120, swe.creation_time)
    LEFT JOIN prime_banner_tapped AS pbt
        ON pbt.p_creation_date BETWEEN ce.p_creation_date AND DATE_ADD('day', 2, ce.p_creation_date)
        AND ce.dynamic_session_id = pbt.dynamic_session_id
        AND pbt.creation_time_local BETWEEN pbs.creation_time_local AND DATE_ADD('minute', 120, pbs.creation_time_local)
*/
    LEFT JOIN order_events AS oe
        ON oe.p_creation_date BETWEEN ce.p_creation_date AND DATE_ADD('day', 2, ce.p_creation_date)
        AND ce.dynamic_session_id = oe.dynamic_session_id
        AND ce.creation_time_local < se.creation_time_local
        AND oe.creation_time_local BETWEEN se.creation_time_local AND DATE_ADD('minute', 120, se.creation_time_local)
        AND se.store_address_id = oe.store_address_id
    LEFT JOIN checkout_events AS che
        ON ce.dynamic_session_id = che.dynamic_session_id
        AND che.p_creation_date BETWEEN ce.p_creation_date AND DATE_ADD('day', 2, ce.p_creation_date)
        AND ce.dynamic_session_id = che.dynamic_session_id
        AND ce.creation_time_local < se.creation_time_local
        AND che.creation_time_local BETWEEN se.creation_time_local AND DATE_ADD('minute', 120, se.creation_time_local)
        AND se.store_address_id = che.store_address_id
    LEFT JOIN glovo_test_groups
        ON COALESCE(ce.customer_id, swe.customer_id, se.customer_id, oe.customer_id, che.customer_id) = glovo_test_groups.customer_id
        --AND ce.creation_time BETWEEN glovo_test_groups.event_time AND glovo_test_groups.end_event_time
    LEFT JOIN top_brands_instore_groups
        ON COALESCE(ce.customer_id, swe.customer_id, se.customer_id, oe.customer_id, che.customer_id) = top_brands_instore_groups.customer_id
    LEFT JOIN delta.central_users_odp.users_v2
        ON users_v2.user_id = ce.customer_id
    LEFT JOIN gnc_migration
        ON ce.customer_id = gnc_migration.customer_id
    GROUP BY 1, 2, 3, 4, 5, 6 
) SELECT *
FROM all_funnel
ORDER BY 1, 2, 3, 4, 5, 6 