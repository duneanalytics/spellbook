{{
  config(
    schema='tokens_solana', 
    alias='pre_computed_transfers', 
    materialized='table',
    partition_by = ['block_month']
  )
}}

SELECT
    call_block_time as block_time
    , cast(date_trunc('day', call_block_time) as date) as block_date
    , cast(date_trunc('month', call_block_time) as date) as block_month
    , cast(date_trunc('year', call_block_time) as date) as block_year
    , call_block_slot as block_slot
    , call_tx_index as tx_index
    , 'transfer' as action
    , amount
    , account_source as from_token_account
    , account_destination as to_token_account
    , SUBSTRING(account_source, 1, 2) as from_token_account_prefix -- Keep precomputed prefix
    , SUBSTRING(account_destination, 1, 2) as to_token_account_prefix -- Keep precomputed prefix
    , 'spl_token' as token_version
    , call_tx_signer as tx_signer
    , call_tx_id as tx_id
    , call_outer_executing_account as outer_executing_account
    -- constructing an artificial instruction_uniq_id to order instructions using one string column
    -- lpads are chosen carefully to be lexicographically sortable
    -- we can sort the instructions by this column to get the correct order
    , CONCAT(
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