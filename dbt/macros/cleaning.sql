{% macro clean_string(col, to_lower=true) -%}
{% if to_lower %}
trim(regexp_replace(lower(cast({{ col }} as string)), r'\s+', ' '))
{% else %}
trim(regexp_replace(cast({{ col }} as string), r'\s+', ' '))
{% endif %}
{%- endmacro %}

{% macro normalize_country_iso2(col) -%}
nullif(
  case
    when length(trim({{ col }})) = 2 then upper(trim({{ col }}))
    else null
  end, ''
)
{%- endmacro %}

{% macro parse_timestamp_safe(col) -%}
cast(nullif(cast({{ col }} as string), '') as timestamp)
{%- endmacro %}

{% macro parse_bool_safe(col) -%}
case
  when lower(cast({{ col }} as string)) in ('true','1','t','y','yes') then true
  when lower(cast({{ col }} as string)) in ('false','0','f','n','no') then false
  else null
end
{%- endmacro %}

{% macro parse_numeric_safe(col) -%}
cast(regexp_replace(cast({{ col }} as string), r'[^0-9\.\-]+', '') as numeric)
{%- endmacro %}

{% macro deduplicate(cte_name, key_cols, order_expr) -%}
select * except(_rn)
from (
  select
    {{ cte_name }}.*,
    row_number() over (partition by {{ key_cols }} order by {{ order_expr }}) as _rn
  from {{ cte_name }}
)
where _rn = 1
{%- endmacro %}

{% macro normalize_txn_sign(type_col, amount_col) -%}
case
  when lower({{ type_col }}) = 'deposit'  then {{ amount_col }}
  when lower({{ type_col }}) = 'withdraw' then -1 * {{ amount_col }}
  else null
end
{%- endmacro %}

{% macro kyc_txn_filter(ts_col, kyc_flag_col, kyc_at_col) -%}
{{ kyc_flag_col }} = true and {{ ts_col }} >= {{ kyc_at_col }}
{%- endmacro %}
