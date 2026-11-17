{{ config(materialized='table',tags=['staging']) }}
SELECT
    TRY_CAST(customerid AS BIGINT) AS customer_id,
    TRIM(customername)             AS customer_name,
    LOWER(TRIM(email))              AS email,
    INITCAP(TRIM(city))             AS city,
    UPPER(TRIM(state))              AS state,
    TRIM(phone)                     AS phone,
    TRY_CAST(createddate AS DATE)  AS created_date
FROM {{ source('raw_source_for_project', 'customers') }}
where customerid is not null