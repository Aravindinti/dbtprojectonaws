{{
  config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    file_format='iceberg'
  )
}}

select
    order_id,
    customer_id,
    amount,
    _loaded_at
from {{ source('raw_source_freshness', 'orders') }}

{% if is_incremental() %}
  where _loaded_at > (select max(_loaded_at) from {{ this }})
{% endif %}