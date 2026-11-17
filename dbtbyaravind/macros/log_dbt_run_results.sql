{% macro log_dbt_run_results(results) %}
    {% if execute %}
        {% for result in results %}
            {% do log(
                'dbt ' ~ result.node.resource_type ~ ' ' ~ result.node.name ~ ': ' ~ result.status,
                info=True
            ) %}
        {% endfor %}
    {% endif %}
{% endmacro %}
