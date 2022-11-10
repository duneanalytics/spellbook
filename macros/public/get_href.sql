{% macro get_href(link, text) %}
      concat('<a href="'
      , replace('{{link}}',"'",'')
      , '"target ="_blank">'
      , replace('{{text}}',"'",'')
      )
{% endmacro %}
