-- this tests checks a perpetual trades model for every row in a seed file.
-- actual implementation in macros/test-helpers/check_seed.sql
{% test check_perpetuals_seed_legacy(model, blockchain=None, project=None, version=None) %}

    {%- set seed_file = ref('perpetual_trades_seed_legacy') -%}
    {%- set seed_check_columns = ['market_address','trade'] -%}
    {%- set seed_matching_columns = ['block_date','blockchain','project','version','tx_hash'] -%}
    {%- set filter = {'blockchain':blockchain, 'project':project, 'version':version} -%}

    {{ check_seed_macro_legacy(model,seed_file,seed_matching_columns,seed_check_columns,filter) }}

{% endtest %}
