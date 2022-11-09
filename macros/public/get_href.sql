{% macro get_href(link, text) %}
      concat('<a href="', '{{link}}', '"target ="_blank">', '{{text}}')
{% endmacro %}
