select
    {{ dbt_utils.generate_surrogate_key(['id', 'user_id']) }} as order_sk,
    *
from {{ source('raw', 'orders') }}