{{ config(materialized='table',tags=['staging']) }}
SELECT
    TRY_CAST(orderid AS BIGINT)    AS order_id,
    TRIM(productid)                AS product_id,
    TRY_CAST(quantity AS BIGINT)    AS quantity
FROM {{ source('raw_source_for_project', 'order_items') }}
WHERE TRY_CAST(orderid AS BIGINT) IS NOT NULL
  AND productid IS NOT NULL
  AND TRIM(productid) <> ''
  AND TRY_CAST(quantity AS BIGINT) IS NOT NULL