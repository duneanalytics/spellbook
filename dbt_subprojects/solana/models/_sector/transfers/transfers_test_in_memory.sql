{{
  config(
    schema='tokens_solana',
    alias='transfers_test_in_memory',
    materialized='table',
    partition_by = ['block_month']
  )
}}

WITH base_transfers AS (
    SELECT
        call_block_time as block_time,
        cast(date_trunc('day', call_block_time) as date) as block_date,
        cast(date_trunc('month', call_block_time) as date) as block_month,
        call_block_slot as block_slot,
        call_tx_index as tx_index,
        'transfer' as action,
        amount,
        account_source as from_token_account,
        account_destination as to_token_account,
        SUBSTRING(account_source, 1, 2) as from_token_account_prefix,
        SUBSTRING(account_destination, 1, 2) as to_token_account_prefix,
        'spl_token' as token_version,
        call_tx_signer as tx_signer,
        call_tx_id as tx_id,
        call_outer_executing_account as outer_executing_account,
        CONCAT(
            lpad(cast(call_block_slot as varchar), 12, '0'), '-',
            lpad(cast(call_tx_index as varchar), 6, '0'), '-',
            lpad(cast(coalesce(call_outer_instruction_index, 0) as varchar), 4, '0'), '-',
            lpad(cast(coalesce(call_inner_instruction_index, 0) as varchar), 4, '0')
          ) AS instruction_uniq_id
    FROM
        {{ source('spl_token_solana', 'spl_token_call_transfer') }}
    WHERE
        1=1
        and call_block_time > TIMESTAMP '2025-04-01'
)

SELECT
    b.block_time,
    b.block_date,
    b.block_month,
    b.action,
    b.amount,
    b.from_token_account,
    b.to_token_account,
    tk_s.token_balance_owner as from_owner,
    tk_d.token_balance_owner as to_owner,
    b.token_version,
    b.tx_signer,
    b.tx_id,
    b.outer_executing_account,
    COALESCE(tk_s.token_mint_address, tk_d.token_mint_address) as token_mint_address
FROM base_transfers b
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