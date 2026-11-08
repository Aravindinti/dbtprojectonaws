{{ config(materialized='table') }}
select
  *
from {{ source('raw', 'orders') }}
where id is not null