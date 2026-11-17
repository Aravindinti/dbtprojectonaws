{{ config(materialized='table',tags=['staging']) }}
SELECT
    TRIM(productid)                AS product_id,
    TRIM(productname)              AS product_name,
    UPPER(TRIM(category))           AS category,
    INITCAP(TRIM(brand))            AS brand,
    TRY_CAST(price AS DECIMAL(10, 2)) AS price
FROM {{ source('raw_source_for_project', 'products') }}
where productid <>'ProductID'