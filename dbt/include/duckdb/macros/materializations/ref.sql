{% macro ref(model_name) %}
    {%- set plugin = config.get('plugin') -%}
    {%- set model_ref = builtins.ref(model_name) -%}

    {% if execute %}
        {% for node in graph.nodes.values() %}
            {% if node.name == model_name %}
                {% set materialization = node.config.materialized %}

                {% if plugin == 'unity' and materialization == 'external_table'%}
                    {% set catalog = var('catalog', 'unity') %}
                    {% set schema = config.get('schema') %}

                    {% if not schema %}
                        {% set schema = 'default' %}
                    {% endif %}

                    {% if schema != 'default' %}
                        -- If the ref already includes schema and model name
                        {{ catalog }}.{{ model_ref[0] }}.{{ model_ref[1] }}
                    {% else %}
                        -- If the ref only includes the model name
                        {{ catalog }}.{{ schema }}.{{ model_ref.identifier }}
                    {% endif %}
                {% else %}
                        {{ model_ref }}
                {% endif %}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}
