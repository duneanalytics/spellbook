 {{
  config(
        schema = 'tokens_solana',
        alias = 'native_transfers',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_id','outer_instruction_index','inner_instruction_index', 'block_slot'],
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "tokens",
                                    \'["ilemi"]\') }}')
}}
--   partition_by = ['block_date'],

--for the reader, note that SOL is special and can be transferred without calling the transfer instruction. It is also minted and burned without instructions. So to get balances, use daily_balances or account_activity instead of transfers.
SELECT
    call_block_time as block_time
--     , date_trunc('day', call_block_time) as block_date
    , call_block_slot as block_slot
    , 'transfer' as action
    , lamports as amount --1e9
    , 'native' as token_mint_address
    , account_from as from_owner
    , account_to as to_owner
    , account_from as from_token_account
    , account_to as to_token_account
    , call_tx_signer as tx_signer
    , call_tx_id as tx_id
    , call_outer_instruction_index as outer_instruction_index
    , COALESCE(call_inner_instruction_index,0) as inner_instruction_index
    , call_outer_executing_account as outer_executing_account
FROM (
      SELECT account_from, account_to, lamports, call_tx_signer, call_block_time, call_block_slot, call_tx_id, call_outer_instruction_index, call_inner_instruction_index, call_outer_executing_account
      FROM {{ source('system_program_solana','system_program_call_Transfer') }}

      UNION ALL 
      
      SELECT account_funding_account, account_recipient_account, lamports, call_tx_signer, call_block_time, call_block_slot, call_tx_id, call_outer_instruction_index, call_inner_instruction_index, call_outer_executing_account
      FROM {{ source('system_program_solana','system_program_call_TransferWithSeed') }}
)
WHERE 1=1
{% if is_incremental() %}
AND {{incremental_predicate('call_block_time')}}
{% endif %}
AND call_block_time > now() - interval '300' day