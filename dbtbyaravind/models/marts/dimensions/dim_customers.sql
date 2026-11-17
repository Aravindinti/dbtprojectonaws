{{ config(materialized='table',tags=['dimensions']) }}
SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS customer_key,

    customer_id,
    customer_name,
    email,
    city,
    state,
    phone,
    created_date

FROM {{ ref('stg_customers') }}