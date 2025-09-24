{% macro scd2_select(source_table, id_col='ID', seq_col='SEQVAL', change_col='CHANGE_INDICATOR', incremental=false) %}

{# 1. Fetch the column list from information_schema dynamically #}
{% set columns_query %}
    select column_name
    from information_schema.columns
    where table_name = upper('{{ source_table }}')
    order by ordinal_position
{% endset %}

{% set results = run_query(columns_query).rows %}

{% set col_list = [] %}
{% for row in results %}
    {% set col_name = row[0] %}
    {% do col_list.append('TRY_CAST(' ~ col_name ~ ' AS ' ~ 'STRING' ~ ') AS ' ~ col_name) %}
{% endfor %}

{% set col_str = col_list | join(', ') %}

{# 2. Build the base CTE #}
with raw as (
    select {{ col_str }}
    from {{ source_table }}
    {% if incremental %}
        where {{ change_col }} in ('INSERT','UPDATE')
    {% endif %}
)

{# 3. Handle SCD2 incremental logic if incremental is true #}
{% if incremental %}
, new_rows as (
    select *,
           CURRENT_TIMESTAMP() as EFFECTIVE_FROM,
           to_timestamp_ltz('9999-12-31') as EFFECTIVE_TO,
           true as CURRENT_FLAG
    from raw
),
expired_rows as (
    select *
    from {{ this }}
    where CURRENT_FLAG = TRUE
      and {{ id_col }} in (select {{ id_col }} from new_rows)
)
select * from expired_rows
union all
select * from new_rows

{% else %}
select *,
       CURRENT_TIMESTAMP() as EFFECTIVE_FROM,
       to_timestamp_ltz('9999-12-31') as EFFECTIVE_TO,
       true as CURRENT_FLAG
from raw
{% endif %}

{% endmacro %}