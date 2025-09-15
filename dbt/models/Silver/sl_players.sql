{{ config(materialized='table') }}

with base as (
  select * from {{ ref('br_players') }}
),
dedup as (
  {{ deduplicate('base','id','updated_at desc, created_at desc') }}
),
kyc_times as (
  select
    id,
    affiliate_id,
    country_code,
    is_kyc_approved,
    created_at,
    updated_at,
    case when is_kyc_approved then coalesce(updated_at, created_at) end as kyc_approved_at
  from dedup
)
select * from kyc_times
