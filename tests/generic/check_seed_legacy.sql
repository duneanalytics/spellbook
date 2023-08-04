-- this tests checks a model for every row in a seed file.
-- you need to specify the matching columns and the columns to check for equality.
-- filter: dictionary filter of column:value that is applied to the seed file
-- actual implementation in macros/test-helpers/check_seed.sql
{% test check_seed_legacy(model, seed_file, match_columns=[], check_columns=[], filter=None) %}
    {#
        --jinja comment
        --    potential dynamic approach, but requires db access -- ci setup to allow in future?
        --    {%- set unique_columns = config.get('unique_key') -%}
        --    {%- set seed_check_columns = dbt_utils.get_filtered_columns_in_relation(from=seed_file, except=unique_columns) -%}
        --    {%- set seed_matching_columns = dbt_utils.get_filtered_columns_in_relation(from=seed_file, except=seed_check_columns) -%}
        --jinja comment
    #}
    {{ config(severity = 'error') }}
    {%- set seed_check_columns = check_columns -%}
    {%- set seed_matching_columns = match_columns -%}
    {%- set seed = seed_file -%}
    {{ check_seed_macro_legacy(model,seed,seed_matching_columns,seed_check_columns,filter) }}

{% endtest %}
