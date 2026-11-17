{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {%- set schema_name = default_schema -%}

    {%- else -%}

        {%- set schema_name = custom_schema_name | trim -%}

    {%- endif -%}

    {%- set iceberg_catalog = target.get('custom_iceberg_catalog_namespace') or 'glue_catalog' -%}
    {%- if node is not none and node.resource_type == 'model' -%}
        {%- if schema_name.startswith(iceberg_catalog ~ '.') -%}
            {{ schema_name }}
        {%- else -%}
            {{ iceberg_catalog }}.{{ schema_name }}
        {%- endif -%}
    {%- else -%}
        {{ schema_name }}
    {%- endif -%}

{%- endmacro %}