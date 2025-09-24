{% macro wca_to_columns(raw_col='raw_line') %}
    {% set metadata_query %}
        select field_name, start_pos, length, type
        from {{ ref('wca_fields') }}
        order by start_pos
    {% endset %}

    {% if execute %}
        {% set results = run_query(metadata_query).rows %}
    {% else %}
        {% set results = [] %}
    {% endif %}

    {% set col_parts = [] %}

    {% for row in results %}
        {% set field_name = row[0] %}
        {% set start_pos  = row[1] %}
        {% set length     = row[2] %}
        {% set field_type = row[3] %}

        {% if field_type == 'text' %}
            {% set part = 'RTRIM(SUBSTR(' ~ raw_col ~ ',' ~ start_pos ~ ',' ~ length ~ ')) AS ' ~ field_name %}
        {% elif field_type == 'number' %}
            {% set part = 'TO_NUMBER(SUBSTR(' ~ raw_col ~ ',' ~ start_pos ~ ',' ~ length ~ ')) AS ' ~ field_name %}
        {% elif field_type == 'date' %}
            {% set part = 'SUBSTR(' ~ raw_col ~ ',' ~ start_pos ~ ',' ~ length ~ ') AS ' ~ field_name %}
        {% else %}
            {% set part = 'SUBSTR(' ~ raw_col ~ ',' ~ start_pos ~ ',' ~ length ~ ') AS ' ~ field_name %}
        {% endif %}

        {% do col_parts.append(part) %}
    {% endfor %}

    {% do col_parts.append('source_file') %}

    {{ return(col_parts | join(',\n')) }}
{% endmacro %}