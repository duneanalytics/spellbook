{% macro dune_link() %}
    create or replace function dune_link(user_ STRING, dashboard_name_ STRING, unique_parameter_ STRING, link_name_ STRING)
    returns STRING 
    return 
        SELECT
            concat(
                '<a href="'
                , 'https://dune.com/'
                , user_
                , '/'
                , dashboard_name_
                , '?'
                , unique_parameter_
                , '"target = "_blank">'
                , link_name_
                , '</a>'
            ) AS dune_link
{% endmacro %}
