{{ config(materialized='table',tags=['staging']) }}
WITH ranked_customers AS (
    SELECT
        TRY_CAST(customerid AS BIGINT) AS customer_id,
        TRIM(customername)             AS customer_name,
        LOWER(TRIM(email))             AS email,
        INITCAP(TRIM(city))            AS city,
        UPPER(TRIM(state))             AS state,
        TRIM(phone)                    AS phone,
        TRY_CAST(createddate AS DATE)  AS created_date,
        ROW_NUMBER() OVER (
            PARTITION BY TRY_CAST(customerid AS BIGINT)
            ORDER BY
                TRY_CAST(createddate AS DATE) DESC NULLS LAST,
                TRIM(customername) ASC NULLS LAST,
                LOWER(TRIM(email)) ASC NULLS LAST,
                INITCAP(TRIM(city)) ASC NULLS LAST,
                UPPER(TRIM(state)) ASC NULLS LAST,
                TRIM(phone) ASC NULLS LAST
        ) AS row_num
    FROM {{ source('raw_source_for_project', 'customers') }}
    WHERE TRY_CAST(customerid AS BIGINT) IS NOT NULL
)
SELECT
    customer_id,
    customer_name,
    email,
    city,
    state,
    phone,
    created_date
FROM ranked_customers
WHERE row_num = 1