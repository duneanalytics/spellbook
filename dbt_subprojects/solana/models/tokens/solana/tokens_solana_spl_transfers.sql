 {{
  config(
        schema = 'tokens_solana',
        alias = 'spl_transfers',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        partition_by = ['block_date'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_id','outer_instruction_index','inner_instruction_index', 'block_slot']
  )
}}


WITH
base as (
      SELECT
            account_source, account_destination
            , bytearray_to_uint256(bytearray_reverse(bytearray_substring(call_data,1+1,8))) as amount
            , call_tx_id, call_block_time, call_block_slot, call_outer_executing_account, call_tx_signer
            , 'transfer' as action
            , call_outer_instruction_index, call_inner_instruction_index
            , cast(null as double) as fee
            , 'spl_token' as token_version
      FROM {{ source('spl_token_solana','spl_token_call_transferChecked') }}
      WHERE 1=1
      {% if is_incremental() %}
      AND {{incremental_predicate('call_block_time')}}
      {% endif %}

      UNION ALL

      SELECT
            null as account_source, account_account as account_destination
            , bytearray_to_uint256(bytearray_reverse(bytearray_substring(call_data,1+1,8))) as amount
            , call_tx_id, call_block_time, call_block_slot, call_outer_executing_account, call_tx_signer
            , 'mint' as action
            , call_outer_instruction_index, call_inner_instruction_index
            , cast(null as double) as fee
            , 'spl_token' as token_version
      FROM {{ source('spl_token_solana','spl_token_call_mintTo') }}
      WHERE 1=1
      {% if is_incremental() %}
      AND {{incremental_predicate('call_block_time')}}
      {% endif %}

      UNION ALL

      SELECT
            null as account_source, account_account as account_destination
            , bytearray_to_uint256(bytearray_reverse(bytearray_substring(call_data,1+1,8))) as amount
            , call_tx_id, call_block_time, call_block_slot, call_outer_executing_account, call_tx_signer
            , 'mint' as action
            , call_outer_instruction_index, call_inner_instruction_index
            , cast(null as double) as fee
            , 'spl_token' as token_version
      FROM {{ source('spl_token_solana','spl_token_call_mintToChecked') }}
      WHERE 1=1
      {% if is_incremental() %}
      AND {{incremental_predicate('call_block_time')}}
      {% endif %}

      UNION ALL

      SELECT
            account_account as account_source, null as account_destination
            , bytearray_to_uint256(bytearray_reverse(bytearray_substring(call_data,1+1,8))) as amount
            , call_tx_id, call_block_time, call_block_slot, call_outer_executing_account, call_tx_signer
            , 'burn' as action
            , call_outer_instruction_index, call_inner_instruction_index
            , cast(null as double) as fee
            , 'spl_token' as token_version
      FROM {{ source('spl_token_solana','spl_token_call_burn') }}
      WHERE 1=1
      {% if is_incremental() %}
      AND {{incremental_predicate('call_block_time')}}
      {% endif %}

      UNION ALL

      SELECT
            account_account as account_source, null as account_destination
            , bytearray_to_uint256(bytearray_reverse(bytearray_substring(call_data,1+1,8))) as amount
            , call_tx_id, call_block_time, call_block_slot, call_outer_executing_account, call_tx_signer
            , 'burn' as action
            , call_outer_instruction_index, call_inner_instruction_index
            , cast(null as double) as fee
            , 'spl_token' as token_version
      FROM {{ source('spl_token_solana','spl_token_call_burnChecked') }}
      WHERE 1=1
      {% if is_incremental() %}
      AND {{incremental_predicate('call_block_time')}}
      {% endif %}
)

SELECT
    call_block_time as block_time
    , cast (date_trunc('day', call_block_time) as date) as block_date
    , call_block_slot as block_slot
    , action
    , amount
    , fee
    , account_source as from_token_account
    , account_destination as to_token_account
    , token_version
    , call_tx_signer as tx_signer
    , call_tx_id as tx_id
    , call_outer_instruction_index as outer_instruction_index
    , COALESCE(call_inner_instruction_index,0) as inner_instruction_index
    , call_outer_executing_account as outer_executing_account
FROM base
UNION ALL
SELECT
    block_time
    , block_date
    , block_slot
    , action
    , amount
    , fee
    , from_token_account
    , to_token_account
    , token_version
    , tx_signer
    , tx_id
    , outer_instruction_index
    , inner_instruction_index
    , outer_executing_account
FROM {{ref('tokens_solana_spl_transfers_call_transfer')}}
{% if is_incremental() %}
    WHERE {{incremental_predicate('block_time')}}
{% endif %}