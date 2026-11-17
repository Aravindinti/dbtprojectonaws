{{ config(materialized='table',tags=['facts']) }}
WITH sales AS (

    SELECT *
    FROM {{ ref('int_sales') }}

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