{{ config(materialized='table') }}

with p as (select * from {{ ref('sl_players') }}),
     a as (select * from {{ ref('sl_affiliates') }}),
     t as (select * from {{ ref('sl_transactions') }}),

j as (
  select
    p.id as player_id,
    p.country_code,
    a.origin,
    t.amount,
    t.type
  from p
  left join a on a.id = p.affiliate_id
  join t on t.player_id = p.id
  where p.is_kyc_approved = true
    and lower(a.origin) = 'discord'
    and lower(t.type) = 'deposit'
)
select
  country_code,
  count(*) as deposit_count,
  sum(amount) as deposit_sum
from j
group by 1
