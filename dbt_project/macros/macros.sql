{% macro calculate_total_revenue(column_name) %}
    sum({{ column_name }})
{% endmacro %}

{% macro set_user_audit_columns() %}
    CAST(current_timestamp() AS TIMESTAMP) as user_created_at,
    CAST(current_timestamp() AS TIMESTAMP) as user_updated_at
{% endmacro %}