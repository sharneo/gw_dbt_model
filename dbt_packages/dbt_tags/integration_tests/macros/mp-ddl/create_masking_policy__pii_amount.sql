{% macro create_masking_policy__pii_amount(ns) -%}

  create masking policy if not exists {{ ns }}.pii_amount as (val number)
    returns number ->
    case
      when is_role_in_session('ROLE_DBT_TAGS') or val is null
        then val
      else -99999
    end;

{%- endmacro %}
