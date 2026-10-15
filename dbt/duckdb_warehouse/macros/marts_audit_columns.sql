{% macro marts_audit_columns() -%}
TIMESTAMP '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f") }}' AS inserted_time,
'dbt' AS inserted_by,
TIMESTAMP '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f") }}' AS updated_time,
'dbt' AS updated_by
{%- endmacro %}
