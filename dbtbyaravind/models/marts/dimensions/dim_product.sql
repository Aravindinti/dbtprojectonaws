{{ config(materialized='table',tags=['dimensions']) }}
SELECT
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_key,

    product_id,
    product_name,
    category,
    brand,
    price

FROM {{ ref('stg_products') }}