{% macro dune_link(asset_id, parameter_id, parameter_input, link_name) %}
     {{
        '<a href="'
        + 'https://dune.com/'
        + asset_id
        + '?'
        + parameter_id
        + '='
        + parameter_input
        + '"target = "_blank">'
        + link_name
        + '</a>'
    }}
{% endmacro %}
