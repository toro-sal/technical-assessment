{{ config(materialized='table') }}

with t as (
  select * from {{ ref('sl_transactions') }}
  where lower(type) = 'deposit'
),
ranked as (
  select
    player_id,
    amount,
    row_number() over (partition by player_id order by amount desc) as rn
  from t
)
select
  player_id,
  amount as top_deposit_amount,
  rn as rank_position
from ranked
where rn <= 3
order by player_id, rn
