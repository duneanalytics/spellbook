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
      {% if not is_incremental() %}
      AND call_block_time > now() - interval '30' day
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
      {% if not is_incremental() %}
      AND call_block_time > now() - interval '30' day
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
      {% if not is_incremental() %}
      AND call_block_time > now() - interval '30' day
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
      {% if not is_incremental() %}
      AND call_block_time > now() - interval '30' day
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
      {% if not is_incremental() %}
      AND call_block_time > now() - interval '30' day
      {% endif %}
)

, prices AS (
    SELECT
        contract_address,
        minute,
        price,
        decimals,
        symbol  -- Add symbol to the prices CTE
    FROM {{ source('prices', 'usd_forward_fill') }}
    WHERE blockchain = 'solana'
    AND minute >= TIMESTAMP '2020-10-02 00:00'
    {% if is_incremental() %}
    AND {{incremental_predicate('minute')}}
    {% endif %}
    {% if not is_incremental() %}
    AND minute > now() - interval '30' day
    {% endif %}
)

SELECT
    tr.call_block_time as block_time
    , cast (date_trunc('day', tr.call_block_time) as date) as block_date
    , tr.call_block_slot as block_slot
    , tr.action
    , tr.amount
    , CASE 
        WHEN p.decimals is null THEN null
        WHEN p.decimals = 0 THEN tr.amount
        ELSE tr.amount / power(10, p.decimals)
      END as amount_display
    , tr.fee
    , tr.account_source as from_token_account
    , tr.account_destination as to_token_account
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
    , p.symbol as symbol
FROM base tr
LEFT JOIN {{ ref('solana_utils_token_accounts') }} tk_s ON tk_s.address = tr.account_source
LEFT JOIN {{ ref('solana_utils_token_accounts') }} tk_d ON tk_d.address = tr.account_destination
LEFT JOIN {{ ref('solana_utils_token_address_mapping') }} tk_m
    ON tk_m.base58_address = COALESCE(tk_s.token_mint_address, tk_d.token_mint_address)
LEFT JOIN prices p
    ON p.contract_address = tk_m.binary_address
    AND p.minute = date_trunc('minute', tr.call_block_time)

UNION ALL

SELECT
    block_time
    , block_date
    , block_slot
    , action
    , amount
    , amount_display
    , fee
    , from_token_account
    , to_token_account
    , from_owner
    , to_owner
    , token_version
    , tx_signer
    , tx_id
    , outer_instruction_index
    , inner_instruction_index
    , outer_executing_account
    , token_mint_address
    , price_usd
    , amount_usd
FROM {{ref('tokens_solana_spl_transfers_call_transfer')}}
{% if is_incremental() %}
WHERE {{incremental_predicate('block_time')}}
{% endif %}
