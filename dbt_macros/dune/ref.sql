{% macro ref(model_name) %}

    {%- if target.schema == 'wizard'  -%}
        {% do return(builtins.ref(model_name).include(database=false)) %}
    {%- else -%}
        {% do return(builtins.ref(model_name)) %}
    {%- endif -%}

{% endmacro %}
