{% macro unix_ms_to_timestamp(column_name) %}
    make_timestamp_ms(cast({{ column_name }} as bigint))
{% endmacro %}