{{
  config(
    schema='tokens_solana', 
    alias='transfers_test', 
    materialized='table',
    partition_by = ['block_month']
  )
}}

-- this is the final model that joins the pre_computed_transfers table with the token_accounts_updates table
-- it is used to test performance of joining on the token_accounts_updates table
-- the token_account upstream model is partitioned by token_account_prefix and block_year
-- that makes these joins efficient

SELECT
    t.block_time
    , t.block_date
    , t.block_month
    , t.action
    , t.amount
    , t.from_token_account
    , t.to_token_account
    , tk_s.token_balance_owner as from_owner
    , tk_d.token_balance_owner as to_owner
    , t.token_version
    , t.tx_signer
    , t.tx_id
    , t.outer_executing_account
    , COALESCE(tk_s.token_mint_address, tk_d.token_mint_address) as token_mint_address
FROM {{ ref('pre_computed_transfers') }} t
LEFT JOIN
    {{ ref('alt_solana_utils_token_accounts_updates') }} tk_s
    ON t.from_token_account_prefix = tk_s.token_account_prefix
    AND t.from_token_account = tk_s.token_account
    AND t.instruction_uniq_id > tk_s.valid_from_instruction_uniq_id
    AND t.instruction_uniq_id < tk_s.valid_to_instruction_uniq_id
    AND t.block_year >= tk_s.valid_from_year
LEFT JOIN
    {{ ref('alt_solana_utils_token_accounts_updates') }} tk_d
    ON t.to_token_account_prefix = tk_d.token_account_prefix
    AND t.to_token_account = tk_d.token_account
    AND t.instruction_uniq_id > tk_d.valid_from_instruction_uniq_id
    AND t.instruction_uniq_id < tk_d.valid_to_instruction_uniq_id
    AND t.block_year >= tk_d.valid_from_year