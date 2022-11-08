{% macro get_href(link, text) %}
      return(concat('<a href="', {{link}}, '"target ="_blank">', {{text}}))
{% endmacro %}
