/*PNA metrics - orders with PNA*/
--v1
    SELECT
        DATE(order_descriptors.order_activated_local_at) AS "order_descriptors.order_activated_local_at",
        TRIM(order_descriptors.store_name)  AS "order_descriptors.store_name",
        order_descriptors.store_address_id  AS "order_descriptors.store_address_id",
        COUNT(DISTINCT bought_products.order_id ) AS "bought_products.number_of_distinct_orders",
        --orders with pna
            COUNT(DISTINCT CASE WHEN (order_descriptors.order_cancel_reason = 'PRODUCTS_NOT_AVAILABLE')
            OR ((order_feedback_facts.selected_option IN ('MISSING_PRODUCTS', 'WRONG_PRODUCTS')) = True)
            OR ((order_refund_incidents.reason IN ('MISSING_PRODUCTS', 'WRONG_PRODUCTS')) = True)
            OR (((((CASE WHEN order_descriptors.order_parent_relationship_type = 'SPLIT'
                    THEN 0
                    ELSE bought_products.bought_product_quantity
                    END) > 0
            AND bought_products.replaced_by_bought_product_id is NOT NULL
            AND order_descriptors.order_final_status = 'DeliveredStatus')
            AND (bought_products_additions_removals_replacements.product_id !=
                bought_products.product_id))) = True)
            OR (((((CASE WHEN order_descriptors.order_parent_relationship_type = 'SPLIT'
                    THEN 0
                    ELSE bought_products.bought_product_quantity
                    END) > 0
            AND bought_products.replaced_by_bought_product_id is NOT NULL
            AND order_descriptors.order_final_status = 'DeliveredStatus')
            AND (bought_products_additions_removals_replacements.bought_product_quantity < (CASE WHEN order_descriptors.order_parent_relationship_type = 'SPLIT'
                    THEN 0
                    ELSE bought_products.bought_product_quantity
                    END))
            AND (bought_products_additions_removals_replacements.bought_product_quantity > 0))) = True)
            OR (((((CASE WHEN order_descriptors.order_parent_relationship_type = 'SPLIT'
                    THEN 0
                    ELSE bought_products.bought_product_quantity
                    END) > 0 AND bought_products.replaced_by_bought_product_id is NOT NULL
            AND order_descriptors.order_final_status = 'DeliveredStatus')
            AND (bought_products_additions_removals_replacements.bought_product_quantity = 0)
            AND (bought_products_additions_removals_replacements.product_id =
                bought_products.product_id))) = True)  THEN order_descriptors.order_id  ELSE NULL END) AS "bought_products_additions_removals_replacements.number_of_orders_with_pna_1"
    FROM delta.customer_bought_products_odp.bought_products  AS bought_products
    LEFT JOIN delta.central_order_descriptors_odp.order_descriptors_v2  AS order_descriptors ON order_descriptors.order_id = bought_products.order_id
    LEFT JOIN delta.dwh_public.order_feedback_facts  AS order_feedback_facts ON order_feedback_facts.bought_product_id = bought_products.bought_product_id
    LEFT JOIN delta.contact_order_refund_incidents_odp.order_refund_incidents  AS order_refund_incidents ON order_descriptors.order_id = order_refund_incidents.order_id
    LEFT JOIN delta.customer_bought_products_odp.bought_products  AS bought_products_additions_removals_replacements ON bought_products.replaced_by_bought_product_id =
        bought_products_additions_removals_replacements.bought_product_id
    WHERE 
        (order_descriptors.store_address_id ) IN ({store_addresses})
        AND ((( order_descriptors.order_activated_local_at  ) >= (TIMESTAMP '{start_date}') 
        AND ( order_descriptors.order_activated_local_at  ) <= (TIMESTAMP '{end_date}')))
    GROUP BY
        1, 2,3
    ORDER BY
        3 DESC
--end_v1

/*PNA metrics - number of products with PNA and UIPO*/
--v2
    SELECT
        order_descriptors.store_address_id  AS "order_descriptors.store_address_id",
        (DATE_FORMAT(order_descriptors.order_activated_local_at , '%Y-%m-%d')) AS "order_descriptors.order_activated_local_date",
        -- number of products with pna
            COUNT(DISTINCT CASE WHEN ((order_feedback_facts.selected_option IN ('MISSING_PRODUCTS', 'WRONG_PRODUCTS')) = True)
            OR (((((CASE WHEN order_descriptors.order_parent_relationship_type = 'SPLIT'
                    THEN 0
                    ELSE bought_products.bought_product_quantity
                    END) > 0
            AND bought_products.replaced_by_bought_product_id is NOT NULL
            AND order_descriptors.order_final_status = 'DeliveredStatus')
            AND (bought_products_additions_removals_replacements.product_id !=
                bought_products.product_id))) = True)
            OR (((((CASE WHEN order_descriptors.order_parent_relationship_type = 'SPLIT'
                    THEN 0
                    ELSE bought_products.bought_product_quantity
                    END) > 0
            AND bought_products.replaced_by_bought_product_id is NOT NULL
            AND order_descriptors.order_final_status = 'DeliveredStatus')
            AND (bought_products_additions_removals_replacements.bought_product_quantity < (CASE WHEN order_descriptors.order_parent_relationship_type = 'SPLIT'
                    THEN 0
                    ELSE bought_products.bought_product_quantity
                    END))
            AND (bought_products_additions_removals_replacements.bought_product_quantity > 0))) = True)
            OR (((((CASE WHEN order_descriptors.order_parent_relationship_type = 'SPLIT'
                    THEN 0
                    ELSE bought_products.bought_product_quantity
                    END) > 0 AND bought_products.replaced_by_bought_product_id is NOT NULL
            AND order_descriptors.order_final_status = 'DeliveredStatus')
            AND (bought_products_additions_removals_replacements.bought_product_quantity = 0)
            AND (bought_products_additions_removals_replacements.product_id =
                bought_products.product_id))) = True)  THEN bought_products.bought_product_id  ELSE NULL END) AS number_of_products_with_pna,
        -- number of placed products
            COUNT(DISTINCT CASE WHEN bought_products.replaced_by_bought_product_id IS NULL THEN bought_products.bought_product_id ELSE NULL END) AS number_of_placed_products,
        -- number of orders
            COUNT(DISTINCT bought_products.order_id)  AS number_of_orders,
        -- number of delivered orders
            COUNT(DISTINCT CASE WHEN (order_descriptors.order_final_status = 'DeliveredStatus') THEN order_descriptors.order_id  ELSE NULL END) AS number_of_delivered_orders,
        -- number of quantity placed
            COALESCE(SUM(CASE WHEN  bought_products.replaced_bought_product_id   IS NULL THEN ( CASE WHEN order_descriptors.order_parent_relationship_type = 'SPLIT'
              THEN 0 ELSE bought_products.bought_product_quantity END) END ), 0) AS total_quantity_placed,
        -- products value placed
            COALESCE(SUM(CASE WHEN bought_products.replaced_bought_product_id IS NULL
                            THEN ((CASE WHEN order_descriptors.order_parent_relationship_type = 'SPLIT'
                            THEN 0 ELSE bought_products.bought_product_quantity
                        END) * ( bought_products.product_unit_price * order_descriptors.order_exchange_rate_to_eur  )) END ), 0) AS products_value_placed
    FROM delta.customer_bought_products_odp.bought_products  AS bought_products
        LEFT JOIN delta.central_order_descriptors_odp.order_descriptors_v2  AS order_descriptors ON order_descriptors.order_id = bought_products.order_id
            AND DATE_TRUNC('month', order_descriptors.p_creation_date) = bought_products.p_creation_month
        LEFT JOIN delta.dwh_public.order_feedback_facts  AS order_feedback_facts ON order_feedback_facts.bought_product_id = bought_products.bought_product_id DEPRECATED
        LEFT JOIN delta.customer_bought_products_odp.bought_products  AS bought_products_additions_removals_replacements ON bought_products.replaced_by_bought_product_id =
            bought_products_additions_removals_replacements.bought_product_id
    WHERE 1=1
        AND (order_descriptors.store_address_id ) IN ({store_addresses})
        AND date(order_descriptors.order_activated_local_at) >= date('{start_date}')
        AND date(order_descriptors.order_activated_local_at) <= date('{end_date}')
    GROUP BY
        1,2
    ORDER BY 1 DESC
--end_v2

/*PNA metrics - number of products with PNA and UIPO*/
--v3
    SELECT
        order_descriptors.store_address_id  AS "order_descriptors.store_address_id",
        (DATE_FORMAT(order_descriptors.order_activated_local_at , '%Y-%m-%d')) AS "order_descriptors.order_activated_local_date",
        -- number of products with pna
            COUNT(DISTINCT CASE WHEN ((order_feedback_facts.feedback_selected_option IN ('MISSING_PRODUCTS', 'WRONG_PRODUCTS')) = True)
            OR (((((CASE WHEN order_descriptors.order_parent_relationship_type = 'SPLIT'
                    THEN 0
                    ELSE bought_products.bought_product_quantity
                    END) > 0
            AND bought_products.replaced_by_bought_product_id is NOT NULL
            AND order_descriptors.order_final_status = 'DeliveredStatus')
            AND (bought_products_additions_removals_replacements.product_id !=
                bought_products.product_id))) = True)
            OR (((((CASE WHEN order_descriptors.order_parent_relationship_type = 'SPLIT'
                    THEN 0
                    ELSE bought_products.bought_product_quantity
                    END) > 0
            AND bought_products.replaced_by_bought_product_id is NOT NULL
            AND order_descriptors.order_final_status = 'DeliveredStatus')
            AND (bought_products_additions_removals_replacements.bought_product_quantity < (CASE WHEN order_descriptors.order_parent_relationship_type = 'SPLIT'
                    THEN 0
                    ELSE bought_products.bought_product_quantity
                    END))
            AND (bought_products_additions_removals_replacements.bought_product_quantity > 0))) = True)
            OR (((((CASE WHEN order_descriptors.order_parent_relationship_type = 'SPLIT'
                    THEN 0
                    ELSE bought_products.bought_product_quantity
                    END) > 0 AND bought_products.replaced_by_bought_product_id is NOT NULL
            AND order_descriptors.order_final_status = 'DeliveredStatus')
            AND (bought_products_additions_removals_replacements.bought_product_quantity = 0)
            AND (bought_products_additions_removals_replacements.product_id =
                bought_products.product_id))) = True)  THEN bought_products.bought_product_id  ELSE NULL END) AS number_of_products_with_pna,
        -- number of placed products
            COUNT(DISTINCT CASE WHEN bought_products.replaced_by_bought_product_id IS NULL THEN bought_products.bought_product_id ELSE NULL END) AS number_of_placed_products,
        -- number of orders
            COUNT(DISTINCT bought_products.order_id)  AS number_of_orders,
        -- number of delivered orders
            COUNT(DISTINCT CASE WHEN (order_descriptors.order_final_status = 'DeliveredStatus') THEN order_descriptors.order_id  ELSE NULL END) AS number_of_delivered_orders,
        -- number of quantity placed
            COALESCE(SUM(CASE WHEN  bought_products.replaced_bought_product_id   IS NULL THEN ( CASE WHEN order_descriptors.order_parent_relationship_type = 'SPLIT'
              THEN 0 ELSE bought_products.bought_product_quantity END) END ), 0) AS total_quantity_placed,
        -- products value placed
            COALESCE(SUM(CASE WHEN bought_products.replaced_bought_product_id IS NULL
                            THEN ((CASE WHEN order_descriptors.order_parent_relationship_type = 'SPLIT'
                            THEN 0 ELSE bought_products.bought_product_quantity
                        END) * ( bought_products.product_unit_price * order_descriptors.order_exchange_rate_to_eur  )) END ), 0) AS products_value_placed
    FROM delta.customer_bought_products_odp.bought_products  AS bought_products
        LEFT JOIN delta.central_order_descriptors_odp.order_descriptors_v2  AS order_descriptors ON order_descriptors.order_id = bought_products.order_id
            AND DATE_TRUNC('month', order_descriptors.p_creation_date) = bought_products.p_creation_month
        LEFT JOIN delta.contact_contact_intent_odp.fct_contact_intent  AS order_feedback_facts ON order_feedback_facts.bought_product_id = bought_products.bought_product_id
        --LEFT JOIN delta.dwh_public.order_feedback_facts  AS order_feedback_facts ON order_feedback_facts.bought_product_id = bought_products.bought_product_id DEPRECATED
        LEFT JOIN delta.customer_bought_products_odp.bought_products  AS bought_products_additions_removals_replacements ON bought_products.replaced_by_bought_product_id =
            bought_products_additions_removals_replacements.bought_product_id
    WHERE 1=1
        AND (order_descriptors.store_address_id ) IN ({store_addresses})
        AND date(order_descriptors.order_activated_local_at) >= date('{start_date}')
        AND date(order_descriptors.order_activated_local_at) <= date('{end_date}')
    GROUP BY
        1,2
    ORDER BY 1 DESC
--end_v3

/*Product availability metrics - Number of Total available products and Avg number of products*/
--prod_availability
SELECT
        (DATE_FORMAT(product_availability.p_snapshot_date , '%Y-%m-%d')) AS "product_availability.snapshot_date_date",
        product_availability.store_address_id AS "store_address_id",
        COALESCE(CAST( ( SUM(DISTINCT (CAST(FLOOR(COALESCE( product_availability.num_available_products  ,0)*(1000000*1.0)) AS DECIMAL(38,0))) + (CAST(FROM_BASE(SUBSTR(TO_HEX(MD5(CAST(CAST( CONCAT_WS(' | ', CAST(product_availability.store_address_id AS varchar),
        CAST(product_availability.p_snapshot_date AS varchar))   AS VARCHAR) AS VARBINARY))),1,14),16) AS DECIMAL(38, 0)) * CAST(10000000000 AS DECIMAL(38, 0)) + CAST(FROM_BASE(SUBSTR(TO_HEX(MD5(CAST(CAST( CONCAT_WS(' | ', CAST(product_availability.store_address_id AS varchar),
        CAST(product_availability.p_snapshot_date AS varchar))   AS VARCHAR) AS VARBINARY))), 17, 10), 16) AS DECIMAL(38, 0))) ) - SUM(DISTINCT (CAST(FROM_BASE(SUBSTR(TO_HEX(MD5(CAST(CAST( CONCAT_WS(' | ', CAST(product_availability.store_address_id AS varchar),
        CAST(product_availability.p_snapshot_date AS varchar))   AS VARCHAR) AS VARBINARY))),1,14),16) AS DECIMAL(38, 0)) * CAST(10000000000 AS DECIMAL(38, 0)) + CAST(FROM_BASE(SUBSTR(TO_HEX(MD5(CAST(CAST( CONCAT_WS(' | ', CAST(product_availability.store_address_id AS varchar),
        CAST(product_availability.p_snapshot_date AS varchar))   AS VARCHAR) AS VARBINARY))), 17, 10), 16) AS DECIMAL(38, 0)))) )  AS DOUBLE) / CAST((1000000*1.0) AS DOUBLE), 0) AS "product_availability.total_available_products",
        (COALESCE(CAST( ( SUM(DISTINCT (CAST(FLOOR(COALESCE( product_availability.num_products  ,0)*(1000000*1.0)) AS DECIMAL(38,0))) + (CAST(FROM_BASE(SUBSTR(TO_HEX(MD5(CAST(CAST( CONCAT_WS(' | ', CAST(product_availability.store_address_id AS varchar),
        CAST(product_availability.p_snapshot_date AS varchar))   AS VARCHAR) AS VARBINARY))),1,14),16) AS DECIMAL(38, 0)) * CAST(10000000000 AS DECIMAL(38, 0)) + CAST(FROM_BASE(SUBSTR(TO_HEX(MD5(CAST(CAST( CONCAT_WS(' | ', CAST(product_availability.store_address_id AS varchar),
        CAST(product_availability.p_snapshot_date AS varchar))   AS VARCHAR) AS VARBINARY))), 17, 10), 16) AS DECIMAL(38, 0))) ) - SUM(DISTINCT (CAST(FROM_BASE(SUBSTR(TO_HEX(MD5(CAST(CAST( CONCAT_WS(' | ', CAST(product_availability.store_address_id AS varchar),
        CAST(product_availability.p_snapshot_date AS varchar))   AS VARCHAR) AS VARBINARY))),1,14),16) AS DECIMAL(38, 0)) * CAST(10000000000 AS DECIMAL(38, 0)) + CAST(FROM_BASE(SUBSTR(TO_HEX(MD5(CAST(CAST( CONCAT_WS(' | ', CAST(product_availability.store_address_id AS varchar),
        CAST(product_availability.p_snapshot_date AS varchar))   AS VARCHAR) AS VARBINARY))), 17, 10), 16) AS DECIMAL(38, 0)))) )  AS DOUBLE) / CAST((1000000*1.0) AS DOUBLE), 0) / NULLIF(COUNT(DISTINCT CASE WHEN   product_availability.num_products   IS NOT NULL THEN  CONCAT_WS(' | ', CAST(product_availability.store_address_id AS varchar),
        CAST(product_availability.p_snapshot_date AS varchar))   ELSE NULL END), 0)) AS "product_availability.avg_num_products"
    FROM delta.partner_product_availability_odp.summary_product_availability  AS product_availability
    LEFT JOIN delta.partner_stores_odp.store_addresses_v2  AS store_addresses ON product_availability.store_address_id = store_addresses.store_address_id
        AND DATE(product_availability.p_snapshot_date)
        BETWEEN store_addresses.start_date
        AND COALESCE(store_addresses.end_date - interval '1' day ,NOW())
    LEFT JOIN delta.partner_stores_odp.stores_v2  AS stores ON stores.store_id = store_addresses.store_id
        AND DATE(product_availability.p_snapshot_date)
        BETWEEN stores.start_date
        AND COALESCE(stores.end_date - interval '1' day , NOW())
    LEFT JOIN delta.central_geography_odp.cities_snapshot_v2  AS cities_snapshot ON stores.city_code = cities_snapshot.city_code
        AND product_availability.p_snapshot_date = cities_snapshot.p_snapshot_date
    WHERE 
        (product_availability.p_snapshot_date) >= date('{start_date}') 
        AND ( product_availability.p_snapshot_date  ) <= date('{end_date}')
        AND (product_availability.store_address_id ) IN ({store_addresses}) 
        -- explore conditions
        AND ((NOT (store_addresses.store_address_is_deleted ) OR (store_addresses.store_address_is_deleted ) IS NULL) 
        AND (store_addresses.store_address_is_partner )) 
        AND ((stores.store_is_enabled ) 
        AND (NOT (stores.store_is_deleted ) OR (stores.store_is_deleted ) IS NULL) 
        AND (((cities_snapshot.country_code ) <> 'AR' AND (cities_snapshot.country_code ) <> 'BO' AND ((cities_snapshot.country_code ) <> 'BY' AND (cities_snapshot.country_code ) <> 'CL') AND ((cities_snapshot.country_code ) <> 'CO' AND (cities_snapshot.country_code ) <> 'CR' AND ((cities_snapshot.country_code ) <> 'DO' AND ((cities_snapshot.country_code ) <> 'EC' AND (cities_snapshot.country_code ) <> 'EG'))) AND ((cities_snapshot.country_code ) <> 'GT' AND (cities_snapshot.country_code ) <> 'PE' AND ((cities_snapshot.country_code ) <> 'UY' AND ((cities_snapshot.country_code ) <> 'ZA' AND (cities_snapshot.country_code ) <> 'TR')) AND ((cities_snapshot.country_code ) <> 'PR' AND (cities_snapshot.country_code ) <> 'BR' AND ((cities_snapshot.country_code ) <> 'HN' AND ((cities_snapshot.country_code ) <> 'PA' AND (cities_snapshot.country_code ) <> 'FR')))) OR (cities_snapshot.country_code ) IS NULL) 
        AND ((cities_snapshot.is_city_enabled ) )))
        -- explore conditions
    GROUP BY
        1,2
    ORDER BY
        1 DESC
--end_prod_availability