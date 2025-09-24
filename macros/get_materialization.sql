{% macro get_materialization() %}
    {% if var('enable_incremental', false) %}
        {{ return('incremental') }}
    {% else %}
        {{ return('table') }}   {# initial SCD2 full refresh #}
    {% endif %}
{% endmacro %}