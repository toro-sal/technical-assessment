{{ config(
  materialized='table',
  partition_by={'field': 'txn_date', 'data_type': 'date'}
) }}

with t as (select * from {{ ref('sl_transactions') }}),
norm as (
  select
    player_id,
    date(txn_timestamp) as txn_date,
    case when lower(type) = 'deposit'  then amount else 0 end as deposit_amount,
    case when lower(type) = 'withdraw' then -1 * amount else 0 end as withdrawal_amount
  from t
)
select
  player_id,
  txn_date,
  sum(deposit_amount)    as deposits,
  sum(withdrawal_amount) as withdrawals
from norm
group by 1,2
