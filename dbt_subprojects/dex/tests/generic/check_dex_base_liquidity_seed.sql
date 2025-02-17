-- this tests checks a dex base_liquidity model for every row in a seed file.
-- actual implementation in macros/test-helpers/check_seed.sql
{% test check_dex_base_liquidity_seed(model, seed_file, filter=None) %}

    {%- set seed_check_columns = ['block_number','id','salt','token0','token1','amount0_raw','amount1_raw'] -%}
    {%- set seed_matching_columns = ['tx_hash','evt_index'] -%}

    {{ check_seed_macro(model,seed_file,seed_matching_columns,seed_check_columns,filter) }}

{% endtest %}
