{{ config(materialized='table',tags=['intermediate']) }}
SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.order_status,
    oi.product_id,
    oi.quantity

FROM {{ ref('stg_orders') }} o

INNER JOIN {{ ref('stg_order_items') }} oi
    ON o.order_id = oi.order_id