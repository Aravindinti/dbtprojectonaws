{{ config(materialized='table') }}

select
  *
from {{ source('raw', 'payments') }}
where id is not null