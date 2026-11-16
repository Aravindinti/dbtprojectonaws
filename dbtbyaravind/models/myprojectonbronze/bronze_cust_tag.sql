{{ config(materialized='table',tags=['testing_tag'],alias='bronze_customers_with_tags') }}

select
  *
from {{ source('raw', 'customers') }}
where id is not null