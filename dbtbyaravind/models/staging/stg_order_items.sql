{{ config(materialized='table',tags=['staging']) }}
SELECT
    TRY_CAST(orderid AS BIGINT)    AS orderid,
    TRIM(productid)                AS productid,
    TRY_CAST(quantity AS BIGINT)    AS quantity
FROM {{ source('raw_source_for_project', 'order_items') }}
where orderid is not null