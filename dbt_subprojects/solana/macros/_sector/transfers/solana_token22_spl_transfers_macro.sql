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
                  {% if is_incremental() %}
                  AND {{incremental_predicate('tr.call_block_time')}}
                  {% else %}
                  AND tr.call_block_time >= {{start_date}}
                  AND tr.call_block_time < {{end_date}}
                  {% endif %}
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
            {% if is_incremental() %}
            AND {{incremental_predicate('call_block_time')}}
            {% else %}
            AND call_block_time >= {{start_date}}
            AND call_block_time < {{end_date}}
            {% endif %}

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
            {% if is_incremental() %}
            AND {{incremental_predicate('call_block_time')}}
            {% else %}
            AND call_block_time >= {{start_date}}
            AND call_block_time < {{end_date}}
            {% endif %}

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
            {% if is_incremental() %}
            AND {{incremental_predicate('call_block_time')}}
            {% else %}
            AND call_block_time >= {{start_date}}
            AND call_block_time < {{end_date}}
            {% endif %}

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
            {% if is_incremental() %}
            AND {{incremental_predicate('call_block_time')}}
            {% else %}
            AND call_block_time >= {{start_date}}
            AND call_block_time < {{end_date}}
            {% endif %}

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
            {% if is_incremental() %}
            AND {{incremental_predicate('call_block_time')}}
            {% else %}
            AND call_block_time >= {{start_date}}
            AND call_block_time < {{end_date}}
            {% endif %}
)
, transfers AS (
      SELECT
            substring(account_source, 1, 2) as from_token_account_prefix
            , account_source as from_token_account
            , substring(account_destination, 1, 2) as to_token_account_prefix
            , account_destination as to_token_account
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
            , concat(
                  lpad(cast(call_block_slot as varchar), 12, '0'), '-',
                  lpad(cast(call_tx_index as varchar), 6, '0'), '-',
                  lpad(cast(call_outer_instruction_index as varchar), 4, '0'), '-',
                  lpad(cast(call_inner_instruction_index as varchar), 4, '0')
            ) as unique_instruction_key --block time is not granular enough, build unique key from block_slot, tx_index, outer_instruction_index, inner_instruction_index
      FROM transfers_raw
)
, prices AS (
    SELECT
        contract_address
        , minute
        , price
        , decimals
        , symbol
    FROM 
        {{ source('prices', 'usd_forward_fill') }}
    WHERE 
        blockchain = 'solana'
        AND minute >= TIMESTAMP '2020-10-02 00:00' --solana start date
        {% if is_incremental() %}
        AND {{incremental_predicate('minute')}}
        {% else %}
        AND minute >= {{start_date}}
        AND minute < {{end_date}}
        {% endif %}
)
SELECT
    cast(date_trunc('month', tr.call_block_time) as date) as block_month
    , cast(date_trunc('day', tr.call_block_time) as date) as block_date
    , tr.call_block_time as block_time
    , tr.call_block_slot as block_slot
    , tr.action
    , tr.amount
    , CASE 
        WHEN p.decimals is null THEN null
        WHEN p.decimals = 0 THEN tr.amount
        ELSE tr.amount / power(10, p.decimals)
      END as amount_display
    /*
    , tr.fee
    */
    , tr.from_token_account
    , tr.to_token_account
    , tk_s.token_balance_owner as from_owner
    , tk_d.token_balance_owner as to_owner
    , tr.token_version
    , tr.call_tx_signer as tx_signer
    , tr.call_tx_id as tx_id
    , tr.call_outer_instruction_index as outer_instruction_index
    , COALESCE(tr.call_inner_instruction_index,0) as inner_instruction_index
    , tr.call_outer_executing_account as outer_executing_account
    , COALESCE(tk_s.token_mint_address, tk_d.token_mint_address) as token_mint_address
    , p.price as price_usd
    , CASE 
        WHEN p.decimals is null THEN null
        WHEN p.decimals = 0 THEN p.price * tr.amount
        ELSE p.price * tr.amount / power(10, p.decimals)
      END as amount_usd
    , p.symbol
FROM transfers as tr
LEFT JOIN 
      {{ ref('solana_utils_token_accounts') }} tk_s
      ON tr.from_token_account_prefix = tk_s.address_prefix
      AND tr.from_token_account = tk_s.address
      AND tr.unique_instruction_key >= tk_s.valid_from_unique_instruction_key
      AND tr.unique_instruction_key < tk_s.valid_to_unique_instruction_key
LEFT JOIN 
      {{ ref('solana_utils_token_accounts') }} tk_d 
      ON tr.to_token_account_prefix = tk_d.address_prefix
      AND tr.to_token_account = tk_d.address
      AND tr.unique_instruction_key >= tk_d.valid_from_unique_instruction_key
      AND tr.unique_instruction_key < tk_d.valid_to_unique_instruction_key
LEFT JOIN 
      {{ ref('solana_utils_token_address_mapping') }} tk_m
      ON tk_m.base58_address = COALESCE(tk_s.token_mint_address, tk_d.token_mint_address)
LEFT JOIN prices p
    ON p.contract_address = tk_m.binary_address
    AND p.minute = date_trunc('minute', tr.call_block_time)
{% endmacro %}