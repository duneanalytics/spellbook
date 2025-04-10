{{
  config(
    schema='tokens_solana',
    alias='transfers_test_single_join_to',
    materialized='table',
    partition_by=['block_month']
  )
}}

SELECT
    b.block_time
    , b.block_date
    , b.block_month
    , b.action
    , b.amount
    , b.from_token_account
    , b.to_token_account
    , tk_s.token_balance_owner as to_owner
    -- , tk_d.token_balance_owner as to_owner -- Removed
    , b.token_version
    , b.tx_signer
    , b.tx_id
    , b.outer_executing_account
    -- , COALESCE(tk_s.token_mint_address, tk_d.token_mint_address) as token_mint_address -- Original
    , tk_s.token_mint_address as token_mint_address -- Simplified to only use tk_s
FROM {{ ref('pre_computed_transfers') }} b
-- this join doesn't have the same partitions upstream
LEFT JOIN
    {{ ref('alt_solana_utils_token_accounts_updates') }} tk_s
    ON tk_s.token_account_prefix = b.to_token_account_prefix
    AND tk_s.token_account = b.to_token_account 
    -- Join condition using instruction_uniq_id
    AND b.instruction_uniq_id >= tk_s.valid_from_instruction_uniq_id
    AND b.instruction_uniq_id < tk_s.valid_to_instruction_uniq_id