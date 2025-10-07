{{
  config(
    schema = 'humidifi_solana'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , unique_key = ['block_month', 'surrogate_key']
  )
}}

{% set project_start_date = '2025-05-26' %}

-- humidifi swap data from instruction_calls table
WITH swaps AS (
    SELECT
          block_slot
        , block_date
        , block_time
        , COALESCE(inner_instruction_index,0) AS inner_instruction_index-- adjust to index 0 for direct trades
        , outer_instruction_index
        , inner_executing_account
        , outer_executing_account
        , executing_account
        , is_inner
        , tx_id
        , tx_signer
        , tx_index
        , account_arguments[2] AS pool_id
        , account_arguments[3] AS token_a_vault
        , account_arguments[4] as token_b_vault
    FROM {{ source('solana','instruction_calls') }}
    WHERE 1=1
        AND executing_account = '9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp'
        --AND BYTEARRAY_SUBSTRING(data, 1, 1) = '0xXX' 
                    -- No standardized SWAP discriminator (?), unreliable method of isolating Humidifi swap instructions. See: https://dune.com/queries/5857394 
                    -- Humidifi team recommendation: arguments = 9 for swaps, join on inner_insturction_index +1 & +2 on token transfers.
        AND cardinality(account_arguments) = 9 -- 9 arguments for all swap instructions. 3 arguments for all quote update instructions. No change in this pattern since deployment
        AND tx_success = true
    {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
    {% else %}
        {% if target.schema.startswith('git_dunesql_') %}
        AND block_time >= current_timestamp - interval '7' day
        {% else %}
        AND block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}
    {% endif %}
)

-- Join inner instruction token transfers initiated by amm swap instructions. Humidifi utilizes TOKEN PROGRAM: TRANSFER for all swaps
, transfers AS (        
    SELECT
          s.block_date
        , s.block_time
        , s.block_slot
        , CASE 
            WHEN s.is_inner = false THEN 'direct'
            ELSE s.outer_executing_account
          END as trade_source
        , t.amount token_bought_amount_raw
        , t1.amount token_sold_amount_raw
        , t.account_source as token_bought_vault
        , t1.account_destination as token_sold_vault
        , s.pool_id AS project_program_id
        , s.tx_signer as trader_id 
        , s.tx_id
        , s.outer_instruction_index
        , s.inner_instruction_index 
        , s.tx_index
    FROM swaps s
    INNER JOIN {{ source('spl_token_solana','spl_token_call_transfer') }} t  ON t.call_tx_id = s.tx_id --buy 
        AND t.call_outer_instruction_index = s.outer_instruction_index
        AND t.call_inner_instruction_index = s.inner_instruction_index + 1
        {% if is_incremental() %}
        AND {{ incremental_predicate('t.call_block_time') }}
        {% else %}
        {% if target.schema.startswith('git_dunesql_') %}
        AND t.call_block_time >= current_timestamp - interval '7' day
        {% else %}
        AND t.call_block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}
        {% endif %}
    INNER JOIN {{ source('spl_token_solana','spl_token_call_transfer') }} t1  ON t1.call_tx_id = s.tx_id --sell 
        AND t1.call_outer_instruction_index = s.outer_instruction_index
        AND t1.call_inner_instruction_index = s.inner_instruction_index + 2
        {% if is_incremental() %}
        AND {{ incremental_predicate('t1.call_block_time') }}
        {% else %}
        {% if target.schema.startswith('git_dunesql_') %}
        AND t1.call_block_time >= current_timestamp - interval '7' day
        {% else %}
        AND t1.call_block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}
        {% endif %}
    WHERE 1=1
        --AND t.tx_id = '52Es2KAyZ8vf86uPLYMQXD8uNgDUxqfQi58zT2P4dmRHWgVzMB8b2iJjZ26rfvQCkJHzjNcVKqmmAmJWtbmmQ7Fd' --testing
            
)

-- reduce table size for next join
,vaults AS (
    SELECT DISTINCT token_a_vault as vault FROM swaps
    UNION ALL
    SELECT DISTINCT token_b_vault as vault FROM swaps
)

--Join token data from solana_utils.token_accounts 
,token_info AS (
    SELECT DISTINCT
         v.vault
        , a1.token_mint_address as token_mint_address
    FROM vaults v
    LEFT JOIN {{ ref('solana_utils_token_accounts') }} a1 ON v.vault = a1.address
)

--join token info with transfers to match buy/sell token info
SELECT
      'solana' as blockchain
    , 'humidifi' AS project
    , 1 AS version
    , 'v1' as version_name
    , date_trunc('month',s.block_date) as block_month
    , s.block_time
    , s.block_slot
    , s.block_date
    , s.trade_source
    , s.token_bought_amount_raw
    , s.token_sold_amount_raw
    , CAST(NULL AS DOUBLE) as fee_tier
    , t1.token_mint_address as token_bought_mint_address  
    , t2.token_mint_address as token_sold_mint_address
    , s.token_bought_vault
    , s.token_sold_vault
    , s.project_program_id
    , '9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp' AS project_main_id
    , s.trader_id 
    , s.tx_id
    , s.outer_instruction_index
    , s.inner_instruction_index
    , s.tx_index
    , {{ dbt_utils.generate_surrogate_key(['tx_id', 'tx_index', 'outer_instruction_index', 'inner_instruction_index']) }} as surrogate_key
FROM transfers s
LEFT JOIN token_info t1 ON s.token_bought_vault = t1.vault
LEFT JOIN token_info t2 ON s.token_sold_vault = t2.vault