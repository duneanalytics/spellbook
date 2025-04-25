{% macro solana_token22_spl_transfers_macro(start_date, end_date) %}

/*
      please note that fee columns have been excluded due to upstream dependencies on token_solana_fees_history outputting duplicates per unique key definition and excluded in production
      when the upstream model is fixed, we can revisit and add fee logic back in here
*/

WITH transfers_raw as (
      --token2022. Most mint and account extensions still use the parent transferChecked instruction, hooks are excecuted after and interest-bearing is precalculated.
      SELECT
            account_source
            , account_destination
            , amount
            , call_tx_id
            , call_tx_index
            , call_block_time
            , call_block_slot
            , call_outer_executing_account
            , call_tx_signer
            , action
            , call_outer_instruction_index
            , call_inner_instruction_index
            , token_version
            /*
            , fee
            */
      FROM (
            SELECT
                  account_source
                  , account_destination
                  , bytearray_to_uint256(bytearray_reverse(bytearray_substring(call_data,1+1,8))) as amount
                  , call_tx_id
                  , call_tx_index
                  , call_block_time
                  , call_block_slot
                  , call_outer_executing_account
                  , call_tx_signer
                  , 'transfer' as action
                  , call_outer_instruction_index
                  , call_inner_instruction_index
                  , 'token2022' as token_version
                  /*
                  , least(
                        cast(bytearray_to_uint256(bytearray_reverse(bytearray_substring(call_data,1+1,8))) as double)
                              *cast(f.fee_basis as double)/10000
                        ,f.fee_maximum) as fee
                  , f.fee_time
                  , row_number() over (partition by tr.call_tx_id,  tr.call_outer_instruction_index,  tr.call_inner_instruction_index order by f.fee_time desc) as latest_fee
                  */
            FROM 
                  {{ source('spl_token_2022_solana','spl_token_2022_call_transferChecked') }} tr
            /*
            LEFT JOIN 
                  {{ ref('tokens_solana_fees_history') }} f 
                  ON tr.account_tokenMint = f.account_mint 
                  AND tr.call_block_time >= f.fee_time
            */
            WHERE 
                  1=1
                  {% if is_incremental() or true -%}
                  AND {{incremental_predicate('tr.call_block_time')}}
                  {% else -%}
                  AND tr.call_block_time >= {{start_date}}
                  AND tr.call_block_time < {{end_date}}
                  {% endif -%}
      )
      /*
      WHERE 
            latest_fee = 1
      */

      UNION ALL

      SELECT
            null as account_source
            , account_mintTo as account_destination
            , bytearray_to_uint256(bytearray_reverse(bytearray_substring(call_data,1+1,8))) as amount
            , call_tx_id
            , call_tx_index
            , call_block_time
            , call_block_slot
            , call_outer_executing_account
            , call_tx_signer
            , 'mint' as action
            , call_outer_instruction_index
            , call_inner_instruction_index
            /*
            , null as fee
            */
            , 'token2022' as token_version
      FROM 
            {{ source('spl_token_2022_solana','spl_token_2022_call_mintTo') }}
      WHERE
            1=1
            {% if is_incremental() or true -%}
            AND {{incremental_predicate('call_block_time')}}
            {% else -%}
            AND call_block_time >= {{start_date}}
            AND call_block_time < {{end_date}}
            {% endif -%}

      UNION ALL

      SELECT
            null as account_source
            , account_mintTo as account_destination
            , bytearray_to_uint256(bytearray_reverse(bytearray_substring(call_data,1+1,8))) as amount
            , call_tx_id
            , call_tx_index
            , call_block_time
            , call_block_slot
            , call_outer_executing_account
            , call_tx_signer
            , 'mint' as action
            , call_outer_instruction_index
            , call_inner_instruction_index
            /*
            , null as fee
            */
            , 'token2022' as token_version
      FROM 
            {{ source('spl_token_2022_solana','spl_token_2022_call_mintToChecked') }}
      WHERE 
            1=1
            {% if is_incremental() or true -%}
            AND {{incremental_predicate('call_block_time')}}
            {% else -%}
            AND call_block_time >= {{start_date}}
            AND call_block_time < {{end_date}}
            {% endif -%}

      UNION ALL

      SELECT
            account_burnAccount as account_source, null as account_destination
            , bytearray_to_uint256(bytearray_reverse(bytearray_substring(call_data,1+1,8))) as amount
            , call_tx_id
            , call_tx_index
            , call_block_time
            , call_block_slot
            , call_outer_executing_account
            , call_tx_signer
            , 'burn' as action
            , call_outer_instruction_index
            , call_inner_instruction_index
            /*
            , null as fee
            */
            , 'token2022' as token_version
      FROM 
            {{ source('spl_token_2022_solana','spl_token_2022_call_burn') }}
      WHERE 
            1=1
            {% if is_incremental() or true -%}
            AND {{incremental_predicate('call_block_time')}}
            {% else -%}
            AND call_block_time >= {{start_date}}
            AND call_block_time < {{end_date}}
            {% endif -%}

      UNION ALL

      SELECT
            account_burnAccount as account_source, null as account_destination
            , bytearray_to_uint256(bytearray_reverse(bytearray_substring(call_data,1+1,8))) as amount
            , call_tx_id
            , call_tx_index
            , call_block_time
            , call_block_slot
            , call_outer_executing_account
            , call_tx_signer
            , 'burn' as action
            , call_outer_instruction_index
            , call_inner_instruction_index
            /*
            , null as fee
            */
            , 'token2022' as token_version
      FROM 
            {{ source('spl_token_2022_solana','spl_token_2022_call_burnChecked') }}
      WHERE 
            1=1
            {% if is_incremental() or true -%}
            AND {{incremental_predicate('call_block_time')}}
            {% else -%}
            AND call_block_time >= {{start_date}}
            AND call_block_time < {{end_date}}
            {% endif -%}

      UNION ALL

      SELECT
            call_account_arguments[1] as account_source, call_account_arguments[3] as account_destination
            , bytearray_to_uint256(bytearray_reverse(bytearray_substring(call_data,1+2,8))) as amount
            , call_tx_id
            , call_tx_index
            , call_block_time
            , call_block_slot
            , call_outer_executing_account
            , call_tx_signer
            , 'transfer' as action
            , call_outer_instruction_index
            , call_inner_instruction_index
            /*
            , bytearray_to_uint256(bytearray_reverse(bytearray_substring(call_data, 1+2+8+1,8))) as fee
            */
            , 'token2022' as token_version
      FROM 
            {{ source('spl_token_2022_solana','spl_token_2022_call_transferFeeExtension') }}
      WHERE 
            bytearray_substring(call_data,1,2) = 0x1a01
            {% if is_incremental() or true -%}
            AND {{incremental_predicate('call_block_time')}}
            {% else -%}
            AND call_block_time >= {{start_date}}
            AND call_block_time < {{end_date}}
            {% endif -%}
)
, transfers AS (
      SELECT
            substring(account_source, 1, 2) as from_token_account_prefix
            , account_source as from_token_account
            , substring(account_destination, 1, 2) as to_token_account_prefix
            , account_destination as to_token_account
            , amount
            /*
            , fee
            */
            , call_tx_id as tx_id
            , call_tx_index as tx_index
            , cast(date_trunc('day', call_block_time) as date) as block_date
            , call_block_time as block_time
            , call_block_slot as block_slot
            , call_outer_executing_account as outer_executing_account
            , call_tx_signer as tx_signer
            , action
            , call_outer_instruction_index as outer_instruction_index
            , COALESCE(call_inner_instruction_index,0) as inner_instruction_index
            , token_version
            , concat(
                  lpad(cast(call_block_slot as varchar), 12, '0'), '-',
                  lpad(cast(call_tx_index as varchar), 6, '0'), '-',
                  lpad(cast(call_outer_instruction_index as varchar), 4, '0'), '-',
                  lpad(cast(call_inner_instruction_index as varchar), 4, '0')
            ) as unique_instruction_key --block time is not granular enough, build unique key from block_slot, tx_index, outer_instruction_index, inner_instruction_index
      FROM transfers_raw
)
, final AS (
    select
        transfers.*
    from
        transfers
    {% if is_incremental() -%}
    left join
        {{ this }} as existing
        -- typically only inner_instruction_index is null, but coalesce all to be safe
        on coalesce(existing.block_date, date '9999-12-31') = coalesce(transfers.block_date, date '9999-12-31')
        and coalesce(existing.block_slot, 0) = coalesce(transfers.block_slot, 0)
        and coalesce(existing.tx_index, 0) = coalesce(transfers.tx_index, 0)
        and coalesce(existing.inner_instruction_index, 0) = coalesce(transfers.inner_instruction_index, 0)
        and coalesce(existing.outer_instruction_index, 0) = coalesce(transfers.outer_instruction_index, 0)
        and {{incremental_predicate('existing.block_time')}}
    where
        existing.block_date is null -- only insert new rows
    {% endif -%}
)
select
    *
from
    final
{% endmacro %}