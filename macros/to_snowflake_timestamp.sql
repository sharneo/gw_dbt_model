{% macro to_snowflake_timestamp(ts_column, unit='nanoseconds') %}
  {% if unit == 'seconds' %}
    to_timestamp({{ ts_column }})
  {% elif unit == 'milliseconds' %}
    to_timestamp({{ ts_column }} / 1000)
  {% elif unit == 'microseconds' %}
    to_timestamp({{ ts_column }} / 1000000)
  {% elif unit == 'nanoseconds' %}
    to_timestamp({{ ts_column }} / 1000000000)
  {% else %}
    {{ exceptions.raise_compiler_error("Unsupported unit: " ~ unit) }}
  {% endif %}
{% endmacro %}
