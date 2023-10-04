 {{
  config(
        tags = ['dunesql'],
        schema = 'phoenix_v1',
        alias = alias('trades'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_id', 'outer_instruction_index', 'inner_instruction_index', 'tx_index','block_month'],
        pre_hook='{{ enforce_join_distribution("PARTITIONED") }}',
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "project",
                                    "phoenix",
                                    \'["ilemi","jarryx"]\') }}')
}}

{% set project_start_date = '2023-02-15' %} --grabbed program deployed at time (account created at)

  WITH
  pools as (
        SELECT 
            tkA.symbol as tokenA_symbol
            , length(json_extract_scalar(initializeParams, '$.InitializeParams.numBaseLotsPerBaseUnit')) - 1 as tokenA_decimals --if lot size is 1000, then its 3 decimals
            , ip.account_baseMint as tokenA
            , ip.account_baseVault as tokenAVault
            , tkB.symbol as tokenB_symbol
            , length(json_extract_scalar(initializeParams, '$.InitializeParams.numQuoteLotsPerQuoteUnit')) - 1 as tokenB_decimals
            , ip.account_quoteMint as tokenB
            , ip.account_quoteVault as tokenBVault
            , cast(json_extract_scalar(initializeParams, '$.InitializeParams.takerFeeBps') as double)/100 as fee_tier
            , ip.account_market as pool_id
            , ip.call_tx_id as init_tx
        FROM {{ source('phoenix_v1_solana','phoenix_v1_call_InitializeMarket') }} ip
        LEFT JOIN {{ ref('tokens_solana_fungible') }}  tkA ON tkA.token_mint_address = ip.account_baseMint
        LEFT JOIN {{ ref('tokens_solana_fungible') }}  tkB ON tkB.token_mint_address = ip.account_quoteMint
  )
  
  , logs AS (
    SELECT
      call_tx_id,
      call_is_inner,
      call_block_slot,
      call_block_time,
      call_inner_instruction_index,
      call_outer_instruction_index,
      call_tx_index,
      BYTEARRAY_TO_BIGINT (
        BYTEARRAY_REVERSE (BYTEARRAY_SUBSTRING (l.call_data, 4, 8))
      ) AS seq,
      TO_BASE58 ((BYTEARRAY_SUBSTRING (l.call_data, 28, 32))) AS market,
      cast(BYTEARRAY_TO_BIGINT (
        BYTEARRAY_REVERSE (
          BYTEARRAY_SUBSTRING (
            l.call_data,
            BYTEARRAY_LENGTH (l.call_data) - 23,
            8
          )
        )
      ) as uint256) AS tokenA_filled,
      l.call_outer_instruction_index AS index,
      cast(BYTEARRAY_TO_BIGINT (
        BYTEARRAY_REVERSE (
          BYTEARRAY_SUBSTRING (
            l.call_data,
            BYTEARRAY_LENGTH (l.call_data) - 15,
            8
          )
        )
      ) as uint256) AS tokenB_filled
    FROM
      {{ source('phoenix_v1_solana','phoenix_v1_call_Log')}} AS l
    WHERE 1=1
      --filter for 0 events
      and bytearray_length (l.call_data) > 93
      --instruction is swap
      and BYTEARRAY_TO_BIGINT (
        BYTEARRAY_REVERSE (BYTEARRAY_SUBSTRING (l.call_data, 3, 1))
      ) = 0
      --filter for trade size > 0
      AND BYTEARRAY_TO_BIGINT (
        BYTEARRAY_REVERSE (
          BYTEARRAY_SUBSTRING (
            l.call_data,
            BYTEARRAY_LENGTH (l.call_data) - 23,
            8
          )
        )
      ) > 0
      --filter for event FillSummary
      AND BYTEARRAY_TO_BIGINT (
        BYTEARRAY_REVERSE (
          BYTEARRAY_SUBSTRING (
            l.call_data,
            BYTEARRAY_LENGTH (l.call_data) - 42,
            1
          )
        )
      ) = 6
      {% if is_incremental() %}
      AND {{incremental_predicate('l.call_block_time')}}
      {% endif %}
  ),
  max_log_index AS (
    SELECT
      market,
      seq,
      MAX(index) AS index
    FROM
      logs
    GROUP BY
      market,
      seq
  ),
  filtered_logs AS (
    SELECT
      l.*
    FROM
      logs AS l
      JOIN max_log_index AS m ON l.market = m.market
      AND l.seq = m.seq
      AND l.index = m.index
  ),
  trades as (
    SELECT
        l.call_block_time as block_time
        , 'phoenix' as project 
        , 1 as version 
        , 'solana' as blockchain
        , case when s.call_inner_instruction_index is null then s.call_outer_executing_account 
            else 'direct' end as trade_source
        , case
            when lower(tokenA_symbol) > lower(tokenB_symbol) then concat(tokenB_symbol, '-', tokenA_symbol)
            else concat(tokenA_symbol, '-', tokenB_symbol)
            end as token_pair
        , case when s.side = 1 then COALESCE(tokenB_symbol, tokenB) 
            else COALESCE(tokenA_symbol, tokenA) 
            end as token_bought_symbol
        , case when s.side = 1 then l.tokenB_filled 
            else l.tokenA_filled 
            end as token_bought_amount_raw
        , case when s.side = 1 then l.tokenB_filled/pow(10,p.tokenB_decimals)
            else l.tokenA_filled/pow(10,p.tokenA_decimals) 
            end token_bought_amount
        , case when s.side = 1 then COALESCE(tokenA_symbol, tokenA) 
            else COALESCE(tokenB_symbol, tokenB) 
            end as token_sold_symbol
        , case when s.side = 1 then l.tokenA_filled 
            else l.tokenB_filled 
            end as token_sold_amount_raw
        , case when s.side = 1 then l.tokenA_filled/pow(10,p.tokenA_decimals)
            else l.tokenB_filled/pow(10,p.tokenB_decimals) 
            end token_sold_amount
        , p.pool_id
        , s.account_trader as trader_id
        , s.call_tx_id as tx_id
        , s.call_outer_instruction_index as outer_instruction_index
        , COALESCE(s.call_inner_instruction_index,0) as inner_instruction_index
        , s.call_tx_index as tx_index
        , case when s.side = 1 then p.tokenB 
            else p.tokenA
            end as token_bought_mint_address
        , case when s.side = 1 then p.tokenBVault
            else p.tokenAVault
            end as token_bought_vault
        , case when s.side = 1 then p.tokenA 
            else p.tokenB 
            end as token_sold_mint_address
        , case when s.side = 1 then p.tokenAVault
            else p.tokenBVault
            end as token_sold_vault
        , p.fee_tier
        , row_number() over (partition by seq order by COALESCE(s.call_inner_instruction_index, 0) desc) as recent_swap -- this ties the log to only the most recent swap call
    FROM filtered_logs l
    LEFT JOIN (
        SELECT 
            *
            , 2 * bytearray_to_integer (bytearray_substring (call_data, 3, 1)) - 1 as side --if side = 1 then tokenB was bought, else tokenA was bought 
        FROM {{ source('phoenix_v1_solana','phoenix_v1_call_Swap') }}
        {% if is_incremental() %}
        WHERE {{incremental_predicate('call_block_time')}}
        {% endif %}
    ) s ON s.call_block_slot = l.call_block_slot
        AND s.call_tx_id = l.call_tx_id
        AND s.account_market = l.market
        AND s.call_outer_instruction_index = l.call_outer_instruction_index
        AND COALESCE(s.call_inner_instruction_index, 0) <= COALESCE(l.call_inner_instruction_index,0) --only get swaps before the log call
    JOIN pools p ON l.market = p.pool_id
  )
  
SELECT
    tb.blockchain
    , tb.project 
    , tb.version
    , CAST(date_trunc('month', tb.block_time) AS DATE) as block_month
    , tb.block_time
    , tb.token_pair
    , tb.trade_source
    , tb.token_bought_symbol
    , tb.token_bought_amount
    , tb.token_bought_amount_raw
    , tb.token_sold_symbol
    , tb.token_sold_amount
    , tb.token_sold_amount_raw
    , COALESCE(tb.token_sold_amount * p_sold.price, tb.token_bought_amount * p_bought.price) as amount_usd
    , tb.fee_tier as fee_tier
    , tb.fee_tier * COALESCE(tb.token_sold_amount * p_sold.price, tb.token_bought_amount * p_bought.price) as fee_usd
    , tb.token_sold_mint_address
    , tb.token_bought_mint_address
    , tb.token_sold_vault
    , tb.token_bought_vault
    , tb.pool_id as project_program_id
    , tb.trader_id
    , tb.tx_id
    , tb.outer_instruction_index
    , tb.inner_instruction_index
    , tb.tx_index
FROM trades tb
LEFT JOIN {{ source('prices', 'usd') }} p_bought ON p_bought.blockchain = 'solana' 
    AND date_trunc('minute', tb.block_time) = p_bought.minute 
    AND token_bought_mint_address = toBase58(p_bought.contract_address)
    {% if is_incremental() %}
    AND {{incremental_predicate('p_bought.minute')}}
    {% else %}
    AND p_bought.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold ON p_sold.blockchain = 'solana' 
    AND date_trunc('minute', tb.block_time) = p_sold.minute 
    AND token_sold_mint_address = toBase58(p_sold.contract_address)
    {% if is_incremental() %}
    AND {{incremental_predicate('p_sold.minute')}}
    {% else %}
    AND p_sold.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
WHERE 1=1 
AND recent_swap = 1