-- this tests checks a dex base_trades model for every row in a seed file.
-- actual implementation in macros/test-helpers/check_seed.sql
{% test check_dex_base_trades_seed(model, seed_file, filter=None) %}

    {%- set seed_check_columns = ['block_number','token_bought_address','token_sold_address','token_bought_amount_raw','token_sold_amount_raw'] -%}
    {%- set seed_matching_columns = ['tx_hash','evt_index'] -%}

    {{ check_seed_macro(model,seed_file,seed_matching_columns,seed_check_columns,filter) }}

{% endtest %}
