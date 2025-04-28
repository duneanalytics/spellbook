{{ config(
    tags = ['prod_exclude'],
    schema = 'tokens_solana',
    alias = 'base_sol_transfers',
    materialized = 'view'
) }}

{%- set models = [
    'tokens_solana_sol_transfers_current'
] -%}

{%- for model in models %}
SELECT 
    block_date
    , block_time
    , block_slot
    , tx_id
    , tx_index
    , inner_instruction_index
    , outer_instruction_index
    , tx_signer
    , amount
    , outer_executing_account
    , from_token_account_prefix
    , from_token_account
    , to_token_account_prefix
    , to_token_account
    , token_version
    , unique_instruction_key
FROM 
    {{ ref(model) }}
{%- if not loop.last %}
UNION ALL
{%- endif %}
{%- endfor %}

