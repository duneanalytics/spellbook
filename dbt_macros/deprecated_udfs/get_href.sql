{% macro get_href() %}
      create or replace function get_href(link STRING, text STRING)
      returns STRING 
      return concat('<a href="',link, '"target ="_blank">', text)
{% endmacro %}
