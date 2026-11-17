{{ config(materialized='table',tags=['staging']) }}
WITH ranked_products AS (
    SELECT
        TRIM(productid)                  AS product_id,
        TRIM(productname)                AS product_name,
        UPPER(TRIM(category))            AS category,
        INITCAP(TRIM(brand))             AS brand,
        TRY_CAST(price AS DECIMAL(10, 2)) AS price,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(productid)
            ORDER BY
                CASE WHEN TRIM(productname) IS NOT NULL AND TRIM(productname) <> '' THEN 1 ELSE 0 END
              + CASE WHEN TRIM(category) IS NOT NULL AND TRIM(category) <> '' THEN 1 ELSE 0 END
              + CASE WHEN TRIM(brand) IS NOT NULL AND TRIM(brand) <> '' THEN 1 ELSE 0 END
              + CASE WHEN TRY_CAST(price AS DECIMAL(10, 2)) IS NOT NULL THEN 1 ELSE 0 END DESC,
                TRIM(productname) ASC NULLS LAST,
                UPPER(TRIM(category)) ASC NULLS LAST,
                INITCAP(TRIM(brand)) ASC NULLS LAST,
                TRY_CAST(price AS DECIMAL(10, 2)) ASC NULLS LAST
        ) AS row_num
    FROM {{ source('raw_source_for_project', 'products') }}
    WHERE productid IS NOT NULL
      AND TRIM(productid) <> ''
      AND UPPER(TRIM(productid)) <> 'PRODUCTID'
)
SELECT
    product_id,
    product_name,
    category,
    brand,
    price
FROM ranked_products
WHERE row_num = 1