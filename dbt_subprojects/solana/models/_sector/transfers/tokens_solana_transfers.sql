{{ config(
    schema = 'tokens_solana',
    alias = 'transfers',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["solana"]\',
                                "sector",
                                "tokens_solana",
                                \'["ilemi", "0xBoxer"]\') }}'
) }}

SELECT
    cast(date_trunc('month', block_time) as date) as block_month
    , block_date
    , cast(date_trunc('hour', block_time) as timestamp) as block_hour
    , block_time
    , block_slot
    , action
    , amount
    , amount_display
    , amount_usd
    , price_usd
    , token_mint_address
    , symbol
    , from_owner
    , to_owner
    , from_token_account
    , to_token_account
    , token_version
    , tx_signer
    , tx_id
    , tx_index
    , outer_instruction_index
    , inner_instruction_index
    , outer_executing_account
FROM {{ ref('tokens_solana_spl_transfers') }}

UNION ALL

SELECT
    cast(date_trunc('month', block_time) as date) as block_month
    , block_date
    , cast(date_trunc('hour', block_time) as timestamp) as block_hour
    , block_time
    , block_slot
    , action
    , amount
    , amount_display
    , amount_usd
    , price_usd
    , token_mint_address
    , symbol
    , from_owner
    , to_owner
    , from_token_account
    , to_token_account
    , token_version
    , tx_signer
    , tx_id
    , tx_index
    , outer_instruction_index
    , inner_instruction_index
    , outer_executing_account
FROM {{ ref('tokens_solana_token22_spl_transfers') }}

UNION ALL

SELECT
    cast(date_trunc('month', block_time) as date) as block_month
    , block_date
    , cast(date_trunc('hour', block_time) as timestamp) as block_hour
    , block_time
    , block_slot
    , action
    , amount
    , amount_display
    , amount_usd
    , price_usd
    , token_mint_address
    , symbol
    , from_owner
    , to_owner
    , from_token_account
    , to_token_account
    , token_version
    , tx_signer
    , tx_id
    , tx_index
    , outer_instruction_index
    , inner_instruction_index
    , outer_executing_account
FROM {{ ref('tokens_solana_spl_transfers_call_transfer') }}

UNION ALL

SELECT
    cast(date_trunc('month', block_time) as date) as block_month
    , block_date
    , cast(date_trunc('hour', block_time) as timestamp) as block_hour
    , block_time
    , block_slot
    , action
    , amount
    , amount_display
    , amount_usd
    , price_usd
    , token_mint_address
    , symbol
    , from_owner
    , to_owner
    , from_token_account
    , to_token_account
    , token_version
    , tx_signer
    , tx_id
    , tx_index
    , outer_instruction_index
    , inner_instruction_index
    , outer_executing_account
FROM {{ ref('tokens_solana_sol_transfers') }}