{{
  config(
    schema='tokens_solana', 
    alias='transfers_test', 
    materialized='table',
    partition_by = ['block_month']
  )
}}

WITH base AS (
    SELECT
        call_block_time as block_time
        , cast(date_trunc('day', call_block_time) as date) as block_date
        , cast(date_trunc('month', call_block_time) as date) as block_month
        , call_block_slot as block_slot
        , call_tx_index as tx_index
        , 'transfer' as action
        , amount
        , account_source as from_token_account
        , account_destination as to_token_account
        , 'spl_token' as token_version
        , call_tx_signer as tx_signer
        , call_tx_id as tx_id
        , call_outer_instruction_index as outer_instruction_index
        , COALESCE(call_inner_instruction_index,0) as inner_instruction_index
        , call_outer_executing_account as outer_executing_account
    FROM
        {{ source('spl_token_solana', 'spl_token_call_transfer') }} 
    WHERE
        1=1
        and call_block_time > TIMESTAMP '2025-04-08'
)

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
FROM base b
LEFT JOIN
    {{ ref('alt_solana_utils_token_accounts_updates') }} tk_s
    ON tk_s.token_account_prefix = SUBSTRING(b.from_token_account, 1, 2)
    AND tk_s.token_account = b.from_token_account
    AND (b.block_time, b.block_slot, b.tx_index, b.outer_instruction_index, b.inner_instruction_index)
        >= (tk_s.valid_from, tk_s.valid_from_block_slot, tk_s.valid_from_tx_index, tk_s.valid_from_outer_index, tk_s.valid_from_inner_index)
    AND (b.block_time, b.block_slot, b.tx_index, b.outer_instruction_index, b.inner_instruction_index)
        < (tk_s.valid_to, tk_s.valid_to_block_slot, tk_s.valid_to_tx_index, tk_s.valid_to_outer_index, tk_s.valid_to_inner_index)
LEFT JOIN
    {{ ref('alt_solana_utils_token_accounts_updates') }} tk_d
    ON tk_d.token_account_prefix = SUBSTRING(b.to_token_account, 1, 2)
    AND tk_d.token_account = b.to_token_account
    AND (b.block_time, b.block_slot, b.tx_index, b.outer_instruction_index, b.inner_instruction_index)
        >= (tk_d.valid_from, tk_d.valid_from_block_slot, tk_d.valid_from_tx_index, tk_d.valid_from_outer_index, tk_d.valid_from_inner_index)
    AND (b.block_time, b.block_slot, b.tx_index, b.outer_instruction_index, b.inner_instruction_index)
        < (tk_d.valid_to, tk_d.valid_to_block_slot, tk_d.valid_to_tx_index, tk_d.valid_to_outer_index, tk_d.valid_to_inner_index)