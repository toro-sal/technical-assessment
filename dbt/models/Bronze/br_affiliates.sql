{{ config(materialized='table') }}

with src as (
  select
    cast(id as int64)                 as id,
    {{ clean_string('code', true) }}  as code,
    {{ clean_string('origin', true) }} as origin,
    {{ parse_timestamp_safe('redeemed_at') }} as redeemed_at
  from {{ source('raw','affiliates') }}
)
select * from src
