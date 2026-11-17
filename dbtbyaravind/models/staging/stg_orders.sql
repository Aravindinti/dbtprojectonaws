{{ config(materialized='table',tags=['staging']) }}
SELECT
    TRY_CAST(orderid AS BIGINT)    AS order_id,
    TRY_CAST(customerid AS BIGINT) AS customer_id,
    TRY_CAST(orderdate AS DATE)    AS order_date,
    UPPER(TRIM(status))       AS order_status
FROM {{ source('raw_source_for_project', 'orders') }}
WHERE TRY_CAST(orderid AS BIGINT) IS NOT NULL
  AND TRY_CAST(customerid AS BIGINT) IS NOT NULL