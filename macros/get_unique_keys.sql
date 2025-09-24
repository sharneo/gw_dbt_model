{% macro get_unique_keys(table_name, schema=target.schema) %}
    {% set col_query %}
        select column_name
        from information_schema.columns
        where table_schema = 'L001_GWPC'
          and table_catalog = 'DEV_RAW_DB'
          and table_name   = '{{ table_name | upper }}'
    {% endset %}

    {% if execute %}
        {% set results = run_query(col_query).rows %}
        {% set cols = results | map(attribute=0) | map('upper') | list %}
    {% else %}
        {% set cols = [] %}
    {% endif %}

    {% if 'SEQVAL' in cols %}
        {{ return(['ID','SEQVAL']) }}
    {% elif 'ID' in cols %}
        {{ return(['ID']) }}
    {% else %}
        {{ exceptions.raise_compiler_error("Table " ~ table_name ~ " has no ID or SEQVAL column") }}
    {% endif %}
{% endmacro %}