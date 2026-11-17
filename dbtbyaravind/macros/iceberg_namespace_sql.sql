{% macro iceberg_namespace_sql(schema_name) %}
    {% set catalog = target.get('custom_iceberg_catalog_namespace') or 'glue_catalog' %}
    {% set prefix = catalog ~ '.' %}
    {% if schema_name.startswith(prefix) %}
        {% set namespace = schema_name[prefix | length:] %}
    {% else %}
        {% set namespace = schema_name %}
    {% endif %}
    {{ return('CREATE NAMESPACE IF NOT EXISTS ' ~ catalog ~ '.' ~ namespace) }}
{% endmacro %}
