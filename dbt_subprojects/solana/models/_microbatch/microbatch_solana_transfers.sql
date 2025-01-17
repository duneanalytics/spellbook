{{ config(
    schema = 'microbatch_test',
    alias = 'solana_transfers',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy='microbatch',
    event_time='block_time',
    begin='2025-01-10',
    batch_size='day',
    lookback=1,
    unique_key = ['block_date', 'tx_id', 'outer_instruction_index', 'inner_instruction_index', 'block_slot']
) }}

SELECT
    block_month
    , block_date
    , date_trunc('hour', block_time) as block_hour
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
    , outer_instruction_index
    , inner_instruction_index
    , outer_executing_account
FROM {{ ref('microbatch_solana_spl_transfers') }}

UNION ALL

--SELECT
--    block_month
--    , block_date
--    , date_trunc('hour', block_time) as block_hour
--    , block_time
--    , block_slot
--    , action
--    , amount
--    , amount_display
--    , amount_usd
--    , price_usd
--    , token_mint_address
--    , symbol
--    , from_owner
--    , to_owner
--    , from_token_account
--    , to_token_account
--    , token_version
--    , tx_signer
--    , tx_id
--    , outer_instruction_index
--    , inner_instruction_index
--    , outer_executing_account
--FROM {{ ref('tokens_solana_token22_spl_transfers') }}
--
--UNION ALL

SELECT
    block_month
    , block_date
    , date_trunc('hour', block_time) as block_hour
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
    , outer_instruction_index
    , inner_instruction_index
    , outer_executing_account
FROM {{ ref('microbatch_solana_spl_transfers_call_transfer') }}

--UNION ALL
--
--SELECT
--    block_month
--    , block_date
--    , date_trunc('hour', block_time) as block_hour
--    , block_time
--    , block_slot
--    , action
--    , amount
--    , amount_display
--    , amount_usd
--    , price_usd
--    , token_mint_address
--    , symbol
--    , from_owner
--    , to_owner
--    , from_token_account
--    , to_token_account
--    , token_version
--    , tx_signer
--    , tx_id
--    , outer_instruction_index
--    , inner_instruction_index
--    , outer_executing_account
--FROM {{ ref('tokens_solana_sol_transfers') }}
