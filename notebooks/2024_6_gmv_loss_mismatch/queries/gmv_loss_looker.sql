SELECT
    bought_products.order_id  AS "bought_products.order_id",
    COALESCE(CAST( ( SUM(DISTINCT (CAST(FLOOR(COALESCE( CASE WHEN  bought_products.replaced_bought_product_id   IS NULL
      THEN ( ( CASE WHEN order_descriptors.order_parent_relationship_type = 'SPLIT'
              THEN 0
              ELSE bought_products.bought_product_quantity
              END  ) * ( bought_products.product_unit_price * order_descriptors.order_exchange_rate_to_eur  )) END  ,0)*(1000000*1.0)) AS DECIMAL(38,0))) + (CAST(FROM_BASE(SUBSTR(TO_HEX(MD5(CAST(CAST( bought_products.bought_product_id   AS VARCHAR) AS VARBINARY))),1,14),16) AS DECIMAL(38, 0)) * CAST(10000000000 AS DECIMAL(38, 0)) + CAST(FROM_BASE(SUBSTR(TO_HEX(MD5(CAST(CAST( bought_products.bought_product_id   AS VARCHAR) AS VARBINARY))), 17, 10), 16) AS DECIMAL(38, 0))) ) - SUM(DISTINCT (CAST(FROM_BASE(SUBSTR(TO_HEX(MD5(CAST(CAST( bought_products.bought_product_id   AS VARCHAR) AS VARBINARY))),1,14),16) AS DECIMAL(38, 0)) * CAST(10000000000 AS DECIMAL(38, 0)) + CAST(FROM_BASE(SUBSTR(TO_HEX(MD5(CAST(CAST( bought_products.bought_product_id   AS VARCHAR) AS VARBINARY))), 17, 10), 16) AS DECIMAL(38, 0)))) )  AS DOUBLE) / CAST((1000000*1.0) AS DOUBLE), 0) AS "bought_products.products_value_eur",
    COALESCE(CAST( ( SUM(DISTINCT (CAST(FLOOR(COALESCE(CASE WHEN  (order_descriptors.order_final_status = 'DeliveredStatus')  THEN  CASE WHEN  bought_products.replaced_by_bought_product_id   IS NULL
      THEN (( CASE WHEN order_descriptors.order_parent_relationship_type = 'SPLIT'
              THEN 0
              ELSE bought_products.bought_product_quantity
              END  ) * ( bought_products.product_unit_price * order_descriptors.order_exchange_rate_to_eur  )) END   ELSE NULL END
,0)*(1000000*1.0)) AS DECIMAL(38,0))) + (CAST(FROM_BASE(SUBSTR(TO_HEX(MD5(CAST(CAST(CASE WHEN  (order_descriptors.order_final_status = 'DeliveredStatus')  THEN  bought_products.bought_product_id   ELSE NULL END
 AS VARCHAR) AS VARBINARY))),1,14),16) AS DECIMAL(38, 0)) * CAST(10000000000 AS DECIMAL(38, 0)) + CAST(FROM_BASE(SUBSTR(TO_HEX(MD5(CAST(CAST(CASE WHEN  (order_descriptors.order_final_status = 'DeliveredStatus')  THEN  bought_products.bought_product_id   ELSE NULL END
 AS VARCHAR) AS VARBINARY))), 17, 10), 16) AS DECIMAL(38, 0))) ) - SUM(DISTINCT (CAST(FROM_BASE(SUBSTR(TO_HEX(MD5(CAST(CAST(CASE WHEN  (order_descriptors.order_final_status = 'DeliveredStatus')  THEN  bought_products.bought_product_id   ELSE NULL END
 AS VARCHAR) AS VARBINARY))),1,14),16) AS DECIMAL(38, 0)) * CAST(10000000000 AS DECIMAL(38, 0)) + CAST(FROM_BASE(SUBSTR(TO_HEX(MD5(CAST(CAST(CASE WHEN  (order_descriptors.order_final_status = 'DeliveredStatus')  THEN  bought_products.bought_product_id   ELSE NULL END
 AS VARCHAR) AS VARBINARY))), 17, 10), 16) AS DECIMAL(38, 0)))) )  AS DOUBLE) / CAST((1000000*1.0) AS DOUBLE), 0) AS "bought_products.products_value_delivered_eur",
    COALESCE(CAST( ( SUM(DISTINCT (CAST(FLOOR(COALESCE(CASE WHEN  (order_descriptors.order_final_status = 'DeliveredStatus')  THEN  CASE WHEN  bought_products.replaced_bought_product_id   IS NULL
      THEN ( ( CASE WHEN order_descriptors.order_parent_relationship_type = 'SPLIT'
              THEN 0
              ELSE bought_products.bought_product_quantity
              END  ) * ( bought_products.product_unit_price * order_descriptors.order_exchange_rate_to_eur  )) END   ELSE NULL END
,0)*(1000000*1.0)) AS DECIMAL(38,0))) + (CAST(FROM_BASE(SUBSTR(TO_HEX(MD5(CAST(CAST(CASE WHEN  (order_descriptors.order_final_status = 'DeliveredStatus')  THEN  bought_products.bought_product_id   ELSE NULL END
 AS VARCHAR) AS VARBINARY))),1,14),16) AS DECIMAL(38, 0)) * CAST(10000000000 AS DECIMAL(38, 0)) + CAST(FROM_BASE(SUBSTR(TO_HEX(MD5(CAST(CAST(CASE WHEN  (order_descriptors.order_final_status = 'DeliveredStatus')  THEN  bought_products.bought_product_id   ELSE NULL END
 AS VARCHAR) AS VARBINARY))), 17, 10), 16) AS DECIMAL(38, 0))) ) - SUM(DISTINCT (CAST(FROM_BASE(SUBSTR(TO_HEX(MD5(CAST(CAST(CASE WHEN  (order_descriptors.order_final_status = 'DeliveredStatus')  THEN  bought_products.bought_product_id   ELSE NULL END
 AS VARCHAR) AS VARBINARY))),1,14),16) AS DECIMAL(38, 0)) * CAST(10000000000 AS DECIMAL(38, 0)) + CAST(FROM_BASE(SUBSTR(TO_HEX(MD5(CAST(CAST(CASE WHEN  (order_descriptors.order_final_status = 'DeliveredStatus')  THEN  bought_products.bought_product_id   ELSE NULL END
 AS VARCHAR) AS VARBINARY))), 17, 10), 16) AS DECIMAL(38, 0)))) )  AS DOUBLE) / CAST((1000000*1.0) AS DOUBLE), 0) AS "placed_value",
    COALESCE(CAST( ( SUM(DISTINCT (CAST(FLOOR(COALESCE(CASE WHEN  (order_descriptors.order_cancel_reason = 'PRODUCTS_NOT_AVAILABLE')  THEN  CASE WHEN  bought_products.replaced_bought_product_id   IS NULL
      THEN ( ( CASE WHEN order_descriptors.order_parent_relationship_type = 'SPLIT'
              THEN 0
              ELSE bought_products.bought_product_quantity
              END  ) * ( bought_products.product_unit_price * order_descriptors.order_exchange_rate_to_eur  )) END   ELSE NULL END
,0)*(1000000*1.0)) AS DECIMAL(38,0))) + (CAST(FROM_BASE(SUBSTR(TO_HEX(MD5(CAST(CAST(CASE WHEN  (order_descriptors.order_cancel_reason = 'PRODUCTS_NOT_AVAILABLE')  THEN  bought_products.bought_product_id   ELSE NULL END
 AS VARCHAR) AS VARBINARY))),1,14),16) AS DECIMAL(38, 0)) * CAST(10000000000 AS DECIMAL(38, 0)) + CAST(FROM_BASE(SUBSTR(TO_HEX(MD5(CAST(CAST(CASE WHEN  (order_descriptors.order_cancel_reason = 'PRODUCTS_NOT_AVAILABLE')  THEN  bought_products.bought_product_id   ELSE NULL END
 AS VARCHAR) AS VARBINARY))), 17, 10), 16) AS DECIMAL(38, 0))) ) - SUM(DISTINCT (CAST(FROM_BASE(SUBSTR(TO_HEX(MD5(CAST(CAST(CASE WHEN  (order_descriptors.order_cancel_reason = 'PRODUCTS_NOT_AVAILABLE')  THEN  bought_products.bought_product_id   ELSE NULL END
 AS VARCHAR) AS VARBINARY))),1,14),16) AS DECIMAL(38, 0)) * CAST(10000000000 AS DECIMAL(38, 0)) + CAST(FROM_BASE(SUBSTR(TO_HEX(MD5(CAST(CAST(CASE WHEN  (order_descriptors.order_cancel_reason = 'PRODUCTS_NOT_AVAILABLE')  THEN  bought_products.bought_product_id   ELSE NULL END
 AS VARCHAR) AS VARBINARY))), 17, 10), 16) AS DECIMAL(38, 0)))) )  AS DOUBLE) / CAST((1000000*1.0) AS DOUBLE), 0) AS "pna_canx",
    COALESCE(CAST( ( SUM(DISTINCT (CAST(FLOOR(COALESCE(CASE WHEN  (order_feedback_facts.feedback_selected_option  IN ('MISSING_PRODUCTS', 'WRONG_PRODUCTS'))  THEN  order_refund_incidents.refunded_to_customer_eur   ELSE NULL END
,0)*(1000000*1.0)) AS DECIMAL(38,0))) + (CAST(FROM_BASE(SUBSTR(TO_HEX(MD5(CAST(CAST(CASE WHEN  (order_feedback_facts.feedback_selected_option  IN ('MISSING_PRODUCTS', 'WRONG_PRODUCTS'))  THEN  order_refund_incidents.id   ELSE NULL END
 AS VARCHAR) AS VARBINARY))),1,14),16) AS DECIMAL(38, 0)) * CAST(10000000000 AS DECIMAL(38, 0)) + CAST(FROM_BASE(SUBSTR(TO_HEX(MD5(CAST(CAST(CASE WHEN  (order_feedback_facts.feedback_selected_option  IN ('MISSING_PRODUCTS', 'WRONG_PRODUCTS'))  THEN  order_refund_incidents.id   ELSE NULL END
 AS VARCHAR) AS VARBINARY))), 17, 10), 16) AS DECIMAL(38, 0))) ) - SUM(DISTINCT (CAST(FROM_BASE(SUBSTR(TO_HEX(MD5(CAST(CAST(CASE WHEN  (order_feedback_facts.feedback_selected_option  IN ('MISSING_PRODUCTS', 'WRONG_PRODUCTS'))  THEN  order_refund_incidents.id   ELSE NULL END
 AS VARCHAR) AS VARBINARY))),1,14),16) AS DECIMAL(38, 0)) * CAST(10000000000 AS DECIMAL(38, 0)) + CAST(FROM_BASE(SUBSTR(TO_HEX(MD5(CAST(CAST(CASE WHEN  (order_feedback_facts.feedback_selected_option  IN ('MISSING_PRODUCTS', 'WRONG_PRODUCTS'))  THEN  order_refund_incidents.id   ELSE NULL END
 AS VARCHAR) AS VARBINARY))), 17, 10), 16) AS DECIMAL(38, 0)))) )  AS DOUBLE) / CAST((1000000*1.0) AS DOUBLE), 0) AS "refunds_due_to_wm",
    COUNT(DISTINCT CASE WHEN product_pna.bought_product_id_is_pna_replacement  THEN ( CONCAT(CAST(product_pna.order_id AS VARCHAR), '_',
      CAST(product_pna.product_external_id AS VARCHAR))  )  ELSE NULL END) AS "product_pna.number_of_products_with_replacement",
    COUNT(DISTINCT CASE WHEN product_pna.bought_product_id_is_pna  THEN ( CONCAT(CAST(product_pna.order_id AS VARCHAR), '_',
      CAST(product_pna.product_external_id AS VARCHAR))  )  ELSE NULL END) AS "product_pna.number_of_products_with_pna_1",
    1.0000 *  ( COUNT(DISTINCT CASE WHEN product_pna.bought_product_id_is_pna  THEN (CONCAT(CAST(product_pna.order_id AS VARCHAR), '_',
      CAST(product_pna.product_external_id AS VARCHAR)))  ELSE NULL END) ) / nullif(( COUNT(DISTINCT (CONCAT(CAST(product_pna.order_id AS VARCHAR), '_',
      CAST(product_pna.product_external_id AS VARCHAR))) ) ), 0)  AS "product_pna.percentage_of_products_with_pna"
FROM delta.customer_bought_products_odp.bought_products_v2  AS bought_products
LEFT JOIN delta.central_order_descriptors_odp.order_descriptors_v2  AS order_descriptors ON order_descriptors.order_id = bought_products.order_id
      AND order_descriptors.p_creation_date = bought_products.p_creation_date
LEFT JOIN delta.contact_contact_intent_odp.fct_contact_intent  AS order_feedback_facts ON order_feedback_facts.bought_product_id = bought_products.bought_product_id
LEFT JOIN delta.contact_order_refund_incidents_odp.order_refund_incidents  AS order_refund_incidents ON order_descriptors.order_id = order_refund_incidents.order_id
LEFT JOIN delta.mfc__pna__odp.pna_products_info  AS product_pna ON bought_products.bought_product_id = product_pna.bought_product_id
      AND bought_products.p_creation_date = product_pna.p_creation_date
LEFT JOIN delta.partner_stores_odp.stores_v2  AS stores ON order_descriptors.store_id = stores.store_id
      AND stores.p_end_date is Null
WHERE ((( bought_products.p_creation_date  ) >= ((DATE_ADD('week', -1, DATE_TRUNC('DAY', DATE_ADD('day', (0 - MOD((DAY_OF_WEEK(CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)) % 7) - 1 + 7, 7)), CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)))))) AND ( bought_products.p_creation_date  ) < ((DATE_ADD('week', 1, DATE_ADD('week', -1, DATE_TRUNC('DAY', DATE_ADD('day', (0 - MOD((DAY_OF_WEEK(CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)) % 7) - 1 + 7, 7)), CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP))))))))) AND (bought_products.product_unit_price * order_descriptors.order_exchange_rate_to_eur ) < 998 AND (order_descriptors.order_country_code ) = 'ES' AND (stores.store_subvertical ) = 'QCPartners' AND (stores.store_subvertical2 ) = 'Groceries'
GROUP BY
    1
ORDER BY
    2 DESC