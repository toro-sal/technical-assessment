{{ config(materialized='table') }}

with a as (select * from {{ ref('sl_affiliates') }}),
     p as (select * from {{ ref('sl_players') }}),

bridge as (
  select
    a.id as affiliate_id,
    a.code,
    a.origin,
    a.redeemed_at,
    p.id as player_id
  from a
  left join p
    on p.affiliate_id = a.id
),
flags as (
  select
    *,
    case when redeemed_at is not null and player_id is null then 1 else 0 end as is_violation_unlinked_redeemed,
    count(player_id) over (partition by affiliate_id) as player_ref_count
  from bridge
)
select
  *,
  case when player_ref_count > 1 then 1 else 0 end as is_violation_affiliate_not_1to1
from flags
