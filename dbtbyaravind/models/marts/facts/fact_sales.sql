{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['order_id', 'product_id'],
        on_schema_change='append_new_columns',
        file_format='iceberg',
        tags=['facts'],
        post_hook="ANALYZE TABLE {{ this }} COMPUTE STATISTICS"
    )
}}
WITH sales AS (

    SELECT *
    FROM {{ ref('int_sales') }}

    {% if is_incremental() %}
    WHERE order_date >= date_sub(
        COALESCE(
            (SELECT MAX(order_date) FROM {{ this }}),
            current_date()
        ),
        {{ var('fact_sales_lookback_days', 7) }}
    )
    {% endif %}

),

customer AS (

    SELECT *
    FROM {{ ref('dim_customers') }}

),

product AS (

    SELECT *
    FROM {{ ref('dim_product') }}

)

SELECT

    sales.order_id,
    sales.product_id,
    customer.customer_key,
    product.product_key,
    sales.order_date,
    sales.order_status,
    sales.quantity,
    product.price,
    sales.quantity * product.price AS sales_amount

FROM sales

INNER JOIN customer
    ON sales.customer_id = customer.customer_id

INNER JOIN product
    ON sales.product_id = product.product_id