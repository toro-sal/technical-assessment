{{ config(
  materialized='table',
  partition_by={'field': 'timestamp', 'data_type': 'timestamp'}
) }}

with src as (
  select
    cast(id as int64)                        as id,
    {{ parse_timestamp_safe('timestamp') }}  as timestamp,
    cast(player_id as int64)                 as player_id,
    {{ clean_string('type', true) }}         as type,
    {{ parse_numeric_safe('amount') }}       as amount
  from {{ source('raw','transactions') }}
)
select * from src
