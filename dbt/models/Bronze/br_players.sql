{{ config(materialized='table') }}

with src as (
  select
    cast(id as int64)                             as id,
    cast(affiliate_id as int64)                   as affiliate_id,
    {{ normalize_country_iso2('country_code') }}  as country_code,
    {{ parse_bool_safe('is_kyc_approved') }}      as is_kyc_approved,
    {{ parse_timestamp_safe('created_at') }}      as created_at,
    {{ parse_timestamp_safe('updated_at') }}      as updated_at
  from {{ source('raw','players') }}
)
select * from src
