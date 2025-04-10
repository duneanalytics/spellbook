{{
  config(
    schema='tokens_solana',
    alias='transfers_test_single_join_to',
    pre_hook="SET SESSION hash_join_enabled = false",
    materialized='table',
    partition_by=['block_month']
  )
}}

SELECT
    b.block_time
    , b.block_date
    , b.block_month
    , b.block_slot
    , b.action
    , b.amount
    , b.from_token_account
    , b.to_token_account
    , tk_s.token_balance_owner as to_owner
    -- , tk_d.token_balance_owner as to_owner -- Removed
    , b.token_version
    , b.tx_signer
    , b.tx_id
    , b.outer_instruction_index
    , b.inner_instruction_index
    , b.outer_executing_account
    -- , COALESCE(tk_s.token_mint_address, tk_d.token_mint_address) as token_mint_address -- Original
    , tk_s.token_mint_address as token_mint_address -- Simplified to only use tk_s
FROM {{ ref('pre_computed_transfers') }} b
-- this join doesn't have the same partitions upstream
LEFT JOIN
    {{ ref('alt_solana_utils_token_accounts_updates') }} tk_s
    ON tk_s.token_account_prefix = b.to_token_account_prefix
    AND tk_s.token_account = b.to_token_account 
    AND (b.block_time, b.block_slot, b.tx_index, b.outer_instruction_index, b.inner_instruction_index)
        >= (tk_s.valid_from, tk_s.valid_from_block_slot, tk_s.valid_from_tx_index, tk_s.valid_from_outer_index, tk_s.valid_from_inner_index)
    AND (b.block_time, b.block_slot, b.tx_index, b.outer_instruction_index, b.inner_instruction_index)
        < (tk_s.valid_to, tk_s.valid_to_block_slot, tk_s.valid_to_tx_index, tk_s.valid_to_outer_index, tk_s.valid_to_inner_index)