 {{
  config(
        schema = 'tokens_solana',
        alias = 'token22_spl_transfers',
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
      --token2022. Most mint and account extensions still use the parent transferChecked instruction, hooks are excecuted after and interest-bearing is precalculated.
      SELECT
            account_source, account_destination, amount
            , call_tx_id, call_block_time, call_block_slot, call_outer_executing_account, call_tx_signer
            , action
            , call_outer_instruction_index, call_inner_instruction_index
            , fee
            , token_version
      FROM (
            SELECT
                  account_source, account_destination
                  , bytearray_to_uint256(bytearray_reverse(bytearray_substring(call_data,1+1,8))) as amount --note that interestbearing mints have a different amount methodology, to add later
                  , call_tx_id, call_block_time, call_block_slot, call_outer_executing_account, call_tx_signer
                  , 'transfer' as action
                  , call_outer_instruction_index, call_inner_instruction_index
                  , least(
                        cast(bytearray_to_uint256(bytearray_reverse(bytearray_substring(call_data,1+1,8))) as double)
                              *cast(f.fee_basis as double)/10000
                        ,f.fee_maximum) as fee --we want to take the percent fee on total amount, but not exceed the maximum fee
                  , 'token2022' as token_version
                  , f.fee_time
                  , row_number() over (partition by tr.call_tx_id,  tr.call_outer_instruction_index,  tr.call_inner_instruction_index order by f.fee_time desc) as latest_fee
            FROM {{ source('spl_token_2022_solana','spl_token_2022_call_transferChecked') }} tr
            LEFT JOIN {{ ref('tokens_solana_fees_history') }} f ON tr.account_tokenMint = f.account_mint AND tr.call_block_time >= f.fee_time
            WHERE 1=1
            {% if is_incremental() %}
            AND {{incremental_predicate('tr.call_block_time')}}
            {% endif %}
      ) WHERE latest_fee = 1

      UNION ALL

      SELECT
            null as account_source, account_mintTo as account_destination
            , bytearray_to_uint256(bytearray_reverse(bytearray_substring(call_data,1+1,8))) as amount
            , call_tx_id, call_block_time, call_block_slot, call_outer_executing_account, call_tx_signer
            , 'mint' as action
            , call_outer_instruction_index, call_inner_instruction_index
            , null as fee
            , 'token2022' as token_version
      FROM {{ source('spl_token_2022_solana','spl_token_2022_call_mintTo') }}
      WHERE 1=1
      {% if is_incremental() %}
      AND {{incremental_predicate('call_block_time')}}
      {% endif %}

      UNION ALL

      SELECT
            null as account_source, account_mintTo as account_destination
            , bytearray_to_uint256(bytearray_reverse(bytearray_substring(call_data,1+1,8))) as amount
            , call_tx_id, call_block_time, call_block_slot, call_outer_executing_account, call_tx_signer
            , 'mint' as action
            , call_outer_instruction_index, call_inner_instruction_index
            , null as fee
            , 'token2022' as token_version
      FROM {{ source('spl_token_2022_solana','spl_token_2022_call_mintToChecked') }}
      WHERE 1=1
      {% if is_incremental() %}
      AND {{incremental_predicate('call_block_time')}}
      {% endif %}

      UNION ALL

      SELECT
            account_burnAccount as account_source, null as account_destination
            , bytearray_to_uint256(bytearray_reverse(bytearray_substring(call_data,1+1,8))) as amount
            , call_tx_id, call_block_time, call_block_slot, call_outer_executing_account, call_tx_signer
            , 'burn' as action
            , call_outer_instruction_index, call_inner_instruction_index
            , null as fee
            , 'token2022' as token_version
      FROM {{ source('spl_token_2022_solana','spl_token_2022_call_burn') }}
      WHERE 1=1
      {% if is_incremental() %}
      AND {{incremental_predicate('call_block_time')}}
      {% endif %}

      UNION ALL

      SELECT
            account_burnAccount as account_source, null as account_destination
            , bytearray_to_uint256(bytearray_reverse(bytearray_substring(call_data,1+1,8))) as amount
            , call_tx_id, call_block_time, call_block_slot, call_outer_executing_account, call_tx_signer
            , 'burn' as action
            , call_outer_instruction_index, call_inner_instruction_index
            , null as fee
            , 'token2022' as token_version
      FROM {{ source('spl_token_2022_solana','spl_token_2022_call_burnChecked') }}
      WHERE 1=1
      {% if is_incremental() %}
      AND {{incremental_predicate('call_block_time')}}
      {% endif %}

      --token2022 transferFeeExtension has some extra complications. It's the only extension with its own transferChecked wrapper (confidential transfers will have this too)
      UNION ALL

      SELECT
            call_account_arguments[1] as account_source, call_account_arguments[3] as account_destination
            , bytearray_to_uint256(bytearray_reverse(bytearray_substring(call_data,1+2,8))) as amount
            , call_tx_id, call_block_time, call_block_slot, call_outer_executing_account, call_tx_signer
            , 'transfer' as action
            , call_outer_instruction_index, call_inner_instruction_index
            , bytearray_to_uint256(bytearray_reverse(bytearray_substring(call_data, 1+2+8+1,8))) as fee
            , 'token2022' as token_version
      FROM {{ source('spl_token_2022_solana','spl_token_2022_call_transferFeeExtension') }}
      WHERE bytearray_substring(call_data,1,2) = 0x1a01 --https://github.com/solana-labs/solana-program-library/blob/8f50c6fabc6ec87ada229e923030381f573e0aed/token/program-2022/src/extension/transfer_fee/instruction.rs#L284
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
FROM base tr
-- AND call_block_time > now() - interval '90' day --for faster CI testing