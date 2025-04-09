{{
  config(
    schema='tokens_solana', 
    alias='transfers_test', 
    materialized='table',
    partition_by = ['block_month']
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
    , tk_s.token_balance_owner as from_owner
    , tk_d.token_balance_owner as to_owner
    , b.token_version
    , b.tx_signer
    , b.tx_id
    , b.outer_instruction_index
    , b.inner_instruction_index
    , b.outer_executing_account
    , COALESCE(tk_s.token_mint_address, tk_d.token_mint_address) as token_mint_address
FROM {{ ref('pre_computed_transfers') }} b
LEFT JOIN
    {{ ref('alt_solana_utils_token_accounts_updates') }} tk_s
    ON tk_s.token_account_prefix = b.from_token_account_prefix
    AND tk_s.token_account = b.from_token_account
    AND (b.block_time, b.block_slot, b.tx_index, b.outer_instruction_index, b.inner_instruction_index)
        >= (tk_s.valid_from, tk_s.valid_from_block_slot, tk_s.valid_from_tx_index, tk_s.valid_from_outer_index, tk_s.valid_from_inner_index)
    AND (b.block_time, b.block_slot, b.tx_index, b.outer_instruction_index, b.inner_instruction_index)
        < (tk_s.valid_to, tk_s.valid_to_block_slot, tk_s.valid_to_tx_index, tk_s.valid_to_outer_index, tk_s.valid_to_inner_index)
LEFT JOIN
    {{ ref('alt_solana_utils_token_accounts_updates') }} tk_d
    ON tk_d.token_account_prefix = b.to_token_account_prefix
    AND tk_d.token_account = b.to_token_account
    AND (b.block_time, b.block_slot, b.tx_index, b.outer_instruction_index, b.inner_instruction_index)
        >= (tk_d.valid_from, tk_d.valid_from_block_slot, tk_d.valid_from_tx_index, tk_d.valid_from_outer_index, tk_d.valid_from_inner_index)
    AND (b.block_time, b.block_slot, b.tx_index, b.outer_instruction_index, b.inner_instruction_index)
        < (tk_d.valid_to, tk_d.valid_to_block_slot, tk_d.valid_to_tx_index, tk_d.valid_to_outer_index, tk_d.valid_to_inner_index)