 {{
  config(
        schema = 'tokens_solana',
        alias = 'spl_transfers_call_transfer',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        partition_by = ['block_month'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_id','outer_instruction_index','inner_instruction_index', 'block_slot']
  )
}}

SELECT
      call_block_time as block_time
    , cast (date_trunc('day', call_block_time) as date) as block_date
    , cast (date_trunc('month', call_block_time) as date) as block_month
    , call_block_slot as block_slot
    , 'transfer' as action
    , amount
    , cast(null as double) as fee
    , account_source as from_token_account
    , account_destination as to_token_account
    , 'spl_token' as token_version
    , call_tx_signer as tx_signer
    , call_tx_id as tx_id
    , call_outer_instruction_index as outer_instruction_index
    , COALESCE(call_inner_instruction_index,0) as inner_instruction_index
    , call_outer_executing_account as outer_executing_account
FROM {{ source('spl_token_solana','spl_token_call_transfer') }}
WHERE 1=1
{% if is_incremental() %}
AND {{incremental_predicate('call_block_time')}}
{% endif %}
