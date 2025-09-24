{% macro incremental_filter(table_name, update_col='updateTime') %}
    {% if get_materialization() == 'incremental' %}
        {% set keys = get_unique_keys(table_name) %}
        {% set conditions = [] %}

        {% if 'SEQVAL' in keys %}
            {% do conditions.append("CHANGE_INDICATOR IN ('INSERT','UPDATE')") %}
        {% endif %}

        {% do conditions.append(update_col ~ " > (select max(" ~ update_col ~ ") from {{ this }})") %}

        and {{ conditions | join(' AND ') }}
    {% else %}
        {# full refresh, no filter #}
        {{ return('') }}
    {% endif %}
{% endmacro %}