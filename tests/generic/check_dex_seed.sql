-- this tests checks a dex trades model for every row in a seed file.

{% test check_dex_seed(model, blockchain=None, project=None, version=None) %}

    {{ config(severity = 'warn') }}
    {%- set seed_file = ref('dex_trades_seed') -%}
    {%- set seed_check_columns = ['token_bought_address','token_sold_address'] -%}
    {%- set seed_matching_columns = ['block_date','blockchain','project','version','tx_hash','evt_index'] -%}
    {%- set filter = {'blockchain':blockchain, 'project':project, 'version':version} -%}

    -- actual implementation in macros/tests/check_seed.sql
    {{ check_seed(model,seed_file,seed_matching_columns,seed_check_columns,filter) }}

{% endtest %}
