-- this tests checks a dex  aggregator trades model for every row in a seed file.
-- actual implementation in macros/test-helpers/check_seed.sql
{% test check_dex_aggregator_seed(model, blockchain=None, project=None, version=None) %}

    {%- set seed_file = ref('dex_aggregator_seed') -%}
    {%- set seed_check_columns = ['token_bought_address','token_sold_address'] -%}
    {%- set seed_matching_columns = ['block_date','blockchain','project','version','tx_hash','evt_index','trace_address'] -%}
    {%- set filter = {'blockchain':blockchain, 'project':project, 'version':version} -%}

    {{ check_seed_macro(model,seed_file,seed_matching_columns,seed_check_columns,filter) }}

{% endtest %}