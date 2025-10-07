{{
  config(
    schema = 'tessera_solana'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , unique_key = ['block_month', 'surrogate_key']
  )
}}

{% set project_start_date = '2025-06-12' %}

-- Base swaps from instruction_calls table
WITH tessera_swaps AS (
    SELECT
          block_slot
        , block_date
        , block_time
        , COALESCE(inner_instruction_index,0) as inner_instruction_index-- adjust to index 0 for non aggregated trades. Avoids complex joins downstream
        , outer_instruction_index
        , inner_executing_account
        , outer_executing_account
        , executing_account
        , is_inner
        , tx_id
        , tx_signer
        , tx_index
        , account_arguments[3] AS user
        , account_arguments[2] AS pool_id
        , account_arguments[4] AS token_a_vault
        , account_arguments[5] as token_b_vault
        , bytearray_to_uint256(bytearray_reverse(bytearray_substring(data,1+1,1))) as is_buy
    FROM {{ source('solana','instruction_calls') }}
    WHERE 1=1
        AND executing_account = 'TessVdML9pBGgG9yGks7o4HewRaXVAMuoVj4x83GLQH'
        AND BYTEARRAY_SUBSTRING(data, 1, 1) = 0x10 -- Swap tag/discriminator. See: https://dune.com/queries/5857473
        AND tx_success = true 
    {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
    {% else %}
        AND block_time >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}
)

-- Get both input and output transfers for each swap
    SELECT
          'solana' as blockchain
        , 'tessera' AS project
        , 1 AS version
        , date_trunc('month',s.block_date) as block_month
        , s.block_date
        , s.block_time
        , s.block_slot
        , CASE 
            WHEN s.is_inner = false THEN 'direct'
            ELSE s.outer_executing_account
            END as trade_source
        , t_buy.amount token_bought_amount_raw
        , t_sell.amount token_sold_amount_raw
        , CAST(NULL AS DOUBLE) as fee_tier
        , t_buy.token_mint_address as token_bought_mint_address
        , t_sell.token_mint_address as token_sold_mint_address
        , t_buy.from_token_account as token_bought_vault
        , t_sell.to_token_account as token_sold_vault
        , s.pool_id AS project_program_id
        , 'TessVdML9pBGgG9yGks7o4HewRaXVAMuoVj4x83GLQH' AS project_main_id
        , s.tx_signer as trader_id --s.trader_id
        , s.tx_id
        , s.outer_instruction_index
        , COALESCE(s.inner_instruction_index, 0) as inner_instruction_index
        , s.tx_index
        , {{ dbt_utils.generate_surrogate_key(['s.tx_id', 's.tx_index', 's.outer_instruction_index', 's.inner_instruction_index']) }} as surrogate_key
    FROM tessera_swaps s
    
    -- asset bought transfers
    INNER JOIN {{ ref('tessera_solana_token_transfers') }} t_buy
        ON t_buy.block_slot = s.block_slot
        AND t_buy.tx_index = s.tx_index
        AND t_buy.block_time = s.block_time  
        AND t_buy.outer_instruction_index = s.outer_instruction_index
        AND t_buy.inner_instruction_index = s.inner_instruction_index + 2

        {% if is_incremental() %}
        AND {{ incremental_predicate('t_buy.block_time') }}
        {% else %}
        AND t_buy.block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}
    
    --asset sold transfers
    INNER JOIN {{ ref('tessera_solana_token_transfers') }} t_sell
        ON t_sell.block_slot = s.block_slot
        AND t_sell.tx_index = s.tx_index
        AND t_sell.block_time = s.block_time
        AND t_sell.outer_instruction_index = s.outer_instruction_index
        AND t_sell.inner_instruction_index = s.inner_instruction_index + 1
    
        {% if is_incremental() %}
        AND {{ incremental_predicate('t_sell.block_time') }}
        {% else %}
        AND t_sell.block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}
