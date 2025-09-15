{{ config(materialized='table') }}

with base as (
  select * from {{ ref('br_affiliates') }}
),
dedup as (
  {{ deduplicate('base','id','redeemed_at desc, code asc') }}
)
select * from dedup
