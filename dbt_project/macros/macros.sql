```
{% macro calculate_total_transactions(column_name) %}
    sum({{ column_name }})
{% endmacro %}

{% macro set_audit_columns() %}
    CAST(current_timestamp() AS TIMESTAMP) as created_at,
    CAST(current_timestamp() AS TIMESTAMP) as updated_at,
    CAST(current_timestamp() AS TIMESTAMP) as processed_at
{% endmacro %}
```