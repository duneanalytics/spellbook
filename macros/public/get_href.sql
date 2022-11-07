{% macro get_href(link, text) %}
      SELECT concat('<a href="', link, '"target ="_blank">', text)
{% endmacro %}
