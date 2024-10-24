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

WITH base AS (
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
),

prices AS (
    SELECT
        contract_address,
        minute,
        price,
        decimals
    FROM {{ source('prices', 'usd_forward_fill') }}
    WHERE blockchain = 'solana'
    AND minute >= TIMESTAMP '2020-10-02 00:00'
    {% if is_incremental() %}
    AND {{incremental_predicate('minute')}}
    {% endif %}
)

SELECT
    b.block_time
    , b.block_date
    , b.block_month
    , b.block_slot
    , b.action
    , b.amount
    , CASE  
        WHEN p.decimals is null THEN null
        WHEN p.decimals = 0 THEN b.amount
        ELSE b.amount / power(10, p.decimals)
      END as amount_display
    , b.fee
    , b.from_token_account
    , b.to_token_account
    , b.token_version
    , b.tx_signer
    , b.tx_id
    , b.outer_instruction_index
    , b.inner_instruction_index
    , b.outer_executing_account
    , COALESCE(tk_s.token_mint_address, tk_d.token_mint_address) as token_mint_address
    , p.price as price_usd
    , CASE 
        WHEN p.decimals is null THEN null
        WHEN p.decimals = 0 THEN p.price * b.amount
        ELSE p.price * b.amount / power(10, p.decimals)
      END as amount_usd
FROM base b
LEFT JOIN {{ ref('solana_utils_token_accounts') }} tk_s ON tk_s.address = b.from_token_account
LEFT JOIN {{ ref('solana_utils_token_accounts') }} tk_d ON tk_d.address = b.to_token_account
LEFT JOIN {{ ref('solana_utils_token_address_mapping') }} tk_m
    ON tk_m.base58_address = COALESCE(tk_s.token_mint_address, tk_d.token_mint_address)
LEFT JOIN prices p
    ON p.contract_address = tk_m.binary_address
    AND p.minute = date_trunc('minute', b.block_time)
