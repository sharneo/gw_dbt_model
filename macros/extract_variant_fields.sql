{% macro extract_variant(table_name, raw_col='raw_data', schema=target.schema) %}
    {% if execute %}
        {% set col_query %}
            select column_name
            from information_schema.columns
            where table_schema = 'L001_GWPC'
            and table_catalog = 'DEV_RAW_DB'
              and table_name   = '{{ table_name | upper }}'
        {% endset %}
        {% set cols = run_query(col_query).rows | map(attribute=0) | map('upper') | list %}
    {% else %}
        {% set cols = [] %}
    {% endif %}

    {% set extracted = [] %}
    {% for c in cols %}
        {% if c not in ['RAW_DATA'] %}
            {% set col_expr = raw_col ~ ':"' ~ c ~ '"::' %}
            {% if c in ['ID','SEQVAL','NumPriorLosses','AccountID'] %}
                {% set col_expr = col_expr ~ 'NUMBER' %}
            {% elif c in ['DoNotDestroy','ExcludedFromArchive','DoNotArchive','IsPortalPolicy'] %}
                {% set col_expr = col_expr ~ 'BOOLEAN' %}
            {% elif c in ['IssueDate','CreateTime','UpdateTime'] %}
                {% set col_expr = col_expr ~ 'TIMESTAMP_NTZ' %}
            {% else %}
                {% set col_expr = col_expr ~ 'STRING' %}
            {% endif %}
            {% set col_expr = col_expr ~ ' as ' ~ c %}
            {% do extracted.append(col_expr) %}
        {% endif %}
    {% endfor %}

    {{ return(extracted | join(',\n')) }}
{% endmacro %}