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
    , b.action
    , b.amount
    , b.from_token_account
    , b.to_token_account
    , tk_s.token_balance_owner as from_owner
    , tk_d.token_balance_owner as to_owner
    , b.token_version
    , b.tx_signer
    , b.tx_id
    , b.outer_executing_account
    , COALESCE(tk_s.token_mint_address, tk_d.token_mint_address) as token_mint_address
FROM {{ ref('pre_computed_transfers') }} b
LEFT JOIN
    {{ ref('alt_solana_utils_token_accounts_updates') }} tk_s
    ON tk_s.token_account_prefix = b.from_token_account_prefix
    AND tk_s.token_account = b.from_token_account
    AND b.instruction_uniq_id >= tk_s.valid_from_instruction_uniq_id
    AND b.instruction_uniq_id < tk_s.valid_to_instruction_uniq_id
LEFT JOIN
    {{ ref('alt_solana_utils_token_accounts_updates') }} tk_d
    ON tk_d.token_account_prefix = b.to_token_account_prefix
    AND tk_d.token_account = b.to_token_account
    AND b.instruction_uniq_id >= tk_d.valid_from_instruction_uniq_id
    AND b.instruction_uniq_id < tk_d.valid_to_instruction_uniq_id