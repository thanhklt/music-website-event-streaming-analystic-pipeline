{% macro ensure_sequence(sequence_name) -%}
    CREATE SEQUENCE IF NOT EXISTS intermediate.{{ adapter.quote(sequence_name) }} START 1
{%- endmacro %}
