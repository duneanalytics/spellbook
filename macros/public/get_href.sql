{% macro get_href() %}
      create or replace function get_href(link STRING, text STRING)

      returns STRING 
      as (
            concat('<a href="'
            , replace(link,"'",'')
            , '"target ="_blank">'
            , replace(text,"'",'')
            )
      );
{% endmacro %}
