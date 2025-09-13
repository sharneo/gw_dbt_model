{% macro create_masking_policy__pii_name(ns) -%}

  create masking policy if not exists {{ ns }}.pii_name as (val string)
    returns string ->
    case
      when is_role_in_session('ROLE_DBT_TAGS') or coalesce(val, '') = ''
        then val
      else sha2(val)
    end;

{%- endmacro %}
