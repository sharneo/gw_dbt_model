{% macro wca_to_columns(raw_col='raw_line') %}

{# Query seed table for metadata #}
{% set metadata_query %}
    SELECT field_name, start_pos, length, type
    FROM {{ ref('wca_fields') }}
    ORDER BY start_pos
{% endset %}

{% set results = run_query(metadata_query).rows %}

{% set col_parts = [] %}

{% for row in results %}
    {% set field_name = row[0] %}
    {% set start_pos = row[1] %}
    {% set length = row[2] %}
    {% set field_type = row[3] %}

    {% if field_type == 'text' %}
        {% set part = 'RTRIM(SUBSTR(' ~ raw_col ~ ',' ~ start_pos ~ ',' ~ length ~ ')) AS ' ~ field_name %}
    {% elif field_type == 'number' %}
        {% set part = 'TO_NUMBER(SUBSTR(' ~ raw_col ~ ',' ~ start_pos ~ ',' ~ length ~ ')) AS ' ~ field_name %}
    {% elif field_type == 'date' %}
        {% set part = 'TO_DATE(SUBSTR(' ~ raw_col ~ ',' ~ start_pos ~ ',' ~ length ~ '), ''YYYYMMDD'') AS ' ~ field_name %}
    {% endif %}

    {% do col_parts.append(part) %}
{% endfor %}

{% do col_parts.append('source_file') %}

{{ return(col_parts | join(',\n')) }}

{% endmacro %}
