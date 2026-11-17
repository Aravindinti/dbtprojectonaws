select *
from {{ ref('fact_sales') }}
where sales_amount < 1
