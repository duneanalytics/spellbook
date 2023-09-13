{% macro dune_link() %}
    create or replace function 
        dune_link(
            asset_id_ STRING -- This is inputted as username/dashboard OR queries/1234567
            , parameter_id_ STRING -- This is the special identifier for your parameter found in the URL between either (? or &) and =
            , parameter_ STRING -- The input you are passing to the parameter, e.g. an EOA address
            , link_name_ STRING -- The string you would like to have represent your URL.
        )
    returns STRING 
    return 
        SELECT
            concat(
                '<a href="'
                , 'https://dune.com/'
                , asset_id
                , '?'
                , parameter_id_
                , '='
                , parameter_
                , '"target = "_blank">'
                , link_name_
                , '</a>'
            ) AS dune_link
{% endmacro %}
