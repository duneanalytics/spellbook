-- this tests checks a dex bot_trades model for every row in a seed file.
-- actual implementation in macros/test-helpers/check_seed.sql
{% test check_bot_trades_seed(model, seed_file, blockchain=None, filter=None) %}

    {%- if blockchain == 'solana' -%}
        {%- set seed_matching_columns = ['blockchain', 'project', 'version', 'tx_id', 'tx_index', 'outer_instruction_index', 'inner_instruction_index'] -%}
    {%- else -%}
        {%- set seed_matching_columns = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index',] -%}
    {%- endif -%}

    {%- set seed_check_columns = ['fee_token_amount', 'fee_token_address', 'project_contract_address', 'token_bought_address', 'token_sold_address', 'user'] -%}

    {{ check_seed_macro(model, seed_file, seed_matching_columns,seed_check_columns,filter) }}

{% endtest %}