{% test ref_integrity(model, fk_col, parent_ref, parent_pk) %}
/* strict FK */
with c as (select {{ fk_col }} as fk from {{ model }} where {{ fk_col }} is not null),
     p as (select {{ parent_pk }} as pk from {{ parent_ref }})
select c.* from c left join p on c.fk = p.pk where p.pk is null
{% endtest %}

{% test ref_integrity_nullable(model, fk_col, parent_ref, parent_pk) %}
/* nullable FK */
with c as (select {{ fk_col }} as fk from {{ model }} where {{ fk_col }} is not null),
     p as (select {{ parent_pk }} as pk from {{ parent_ref }})
select c.* from c left join p on c.fk = p.pk where p.pk is null
{% endtest %}

{% test test_fd(model, determinant_cols, dependent_cols) %}
with shaped as (select {{ determinant_cols }}, {{ dependent_cols }} from {{ model }}),
viol as (
  select {{ determinant_cols }},
         count(distinct concat({{ dependent_cols | replace(',','),"|",(') }})) as combos
  from shaped group by {{ determinant_cols }} having combos > 1
)
select * from viol
{% endtest %}

{% test unique_by_key(model, key_cols) %}
with d as (select {{ key_cols }}, count(*) as cnt from {{ model }} group by {{ key_cols }})
select * from d where cnt > 1
{% endtest %}

{% test values_in_ci(model, column_name, allowed) %}
select * from {{ model }}
where lower(cast({{ column_name }} as string)) not in (
  {% for v in allowed %}lower('{{ v }}'){% if not loop.last %}, {% endif %}{% endfor %}
)
{% endtest %}

{% test txn_signs_consistent(model, type_col, amount_col) %}
select * from {{ model }}
where (lower({{ type_col }}) = 'deposit' and {{ amount_col }} < 0)
   or (lower({{ type_col }}) = 'withdraw' and {{ amount_col }} > 0)
{% endtest %}

{% test one_to_one(model, left_col, right_col) %}
with pairs as (
  select {{ left_col }} as l, {{ right_col }} as r
  from {{ model }}
  where {{ left_col }} is not null and {{ right_col }} is not null
),
viol_left as (select l from pairs group by l having count(distinct r) > 1),
viol_right as (select r from pairs group by r having count(distinct l) > 1)
select * from viol_left
union all
select * from viol_right
{% endtest %}

{% test no_txn_before_kyc(model) %}
with t as (select * from {{ model }}),
     p as (select * from {{ ref('sl_players') }})
select t.*
from t join p on p.id = t.player_id
where (p.is_kyc_approved is distinct from true) or (t.timestamp < p.kyc_approved_at)
{% endtest %}
