{% macro get_modified_base_trades() %}
    {% set modified_files = var('MODIFIED_BASE_TRADES', '') %}
    {% set models = [] %}
    {% if modified_files %}
        {% set files = modified_files.split(',') %}
        {% for file in files %}
            {% set model_name = file.split('/')[-1].replace('.sql', '') %}
            {% do models.append(model_name) %}
        {% endfor %}
    {% endif %}
    {{ return(models) }}
{% endmacro %}
