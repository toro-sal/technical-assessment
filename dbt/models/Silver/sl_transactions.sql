{{ config(
  materialized='table',
  partition_by={'field': 'txn_timestamp', 'data_type': 'timestamp'}
) }}

with tr as (
  select
    cast(t.id as int64)        as id,
    t.`timestamp`              as raw_ts,        -- only mention of the column name
    cast(t.player_id as int64) as player_id,
    {{ clean_string('type', true) }}         as txn_type,
    {{ parse_numeric_safe('amount') }}       as amount
  from {{ ref('br_transactions') }} as t
),
typed as (
  select
    id,
    cast(nullif(cast(raw_ts as string), '') as timestamp) as txn_ts,
    player_id,
    txn_type,
    amount
  from tr
),
p as (
  select
    cast(id as int64) as player_id,
    is_kyc_approved,
    kyc_approved_at
  from {{ ref('sl_players') }}
)

select
  typed.id,
  typed.txn_ts   as txn_timestamp,   -- used by partition_by
  typed.player_id,
  typed.txn_type as type,
  typed.amount
from typed
join p
  on p.player_id = typed.player_id
where p.is_kyc_approved = true
  and typed.txn_ts >= p.kyc_approved_at
