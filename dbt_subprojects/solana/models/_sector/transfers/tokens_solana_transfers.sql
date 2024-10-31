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
    block_time
    , block_date
    , date_trunc('hour', block_time) as block_hour
    , block_slot
    , action
    , amount
    , amount_display
    , amount_usd
    , price_usd
    , fee
    , token_mint_address
    , symbol
    , from_owner
    , to_owner
    , from_token_account
    , to_token_account
    , token_version
    , tx_signer
    , tx_id
    , outer_instruction_index
    , inner_instruction_index
    , outer_executing_account
FROM {{ ref('tokens_solana_spl_transfers') }}

UNION ALL

SELECT
    block_time
    , block_date
    , date_trunc('hour', block_time) as block_hour
    , block_slot
    , action
    , amount
    , amount_display
    , amount_usd
    , price_usd
    , fee
    , token_mint_address
    , symbol
    , from_owner
    , to_owner
    , from_token_account
    , to_token_account
    , token_version
    , tx_signer
    , tx_id
    , outer_instruction_index
    , inner_instruction_index
    , outer_executing_account
FROM {{ ref('tokens_solana_token22_spl_transfers') }}

UNION ALL

SELECT
    block_time
    , block_date
    , date_trunc('hour', block_time) as block_hour
    , block_slot
    , action
    , amount
    , amount_display
    , amount_usd
    , price_usd
    , fee
    , token_mint_address
    , symbol
    , from_owner
    , to_owner
    , from_token_account
    , to_token_account
    , token_version
    , tx_signer
    , tx_id
    , outer_instruction_index
    , inner_instruction_index
    , outer_executing_account
FROM {{ ref('tokens_solana_spl_transfers_call_transfer') }}

UNION ALL

SELECT
    block_time
    , block_date
    , date_trunc('hour', block_time) as block_hour
    , block_slot
    , action
    , amount
    , amount_display
    , amount_usd
    , price_usd
    , fee
    , token_mint_address
    , symbol
    , from_owner
    , to_owner
    , cast(null as varchar) as from_token_account
    , cast(null as varchar) as to_token_account
    , token_version
    , tx_signer
    , tx_id
    , outer_instruction_index
    , inner_instruction_index
    , outer_executing_account
FROM {{ ref('tokens_solana_sol_transfers') }}