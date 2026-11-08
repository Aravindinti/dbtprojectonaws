{{ config(materialized='table') }}

select
  *
from {{ source('raw', 'customers') }}
where id is not null