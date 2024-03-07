{{ config(
    schema = 'bebop_rfq_arbitrum',
    alias = 'trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
)}}

{% set project_start_date = '2023-03-30' %}

WITH 

bebop_raw_data AS (
    SELECT
        call_block_time AS block_time,
        call_block_number AS block_number,
        call_tx_hash AS tx_hash,
        evt_index,
        ex.contract_address,
        JSON_EXTRACT_SCALAR(ex."order", '$.expiry') AS expiry,
        from_hex(JSON_EXTRACT_SCALAR(ex."order", '$.taker_address')) as taker_address,
        from_hex(JSON_EXTRACT_SCALAR(JSON_EXTRACT(ex."order", '$.maker_addresses'), '$[0]'))  AS maker_address,
        JSON_EXTRACT(ex."order", '$.taker_tokens') AS taker_tokens_json,
        JSON_EXTRACT(ex."order", '$.maker_tokens') AS maker_tokens_json,
        JSON_EXTRACT(ex."order", '$.taker_amounts') AS taker_amounts_json,
        JSON_EXTRACT(ex."order", '$.maker_amounts') AS maker_amounts_json,
        json_array_length(json_extract((JSON_EXTRACT(ex."order", '$.taker_tokens')), '$[0]')) as taker_length,
        json_array_length(json_extract((JSON_EXTRACT(ex."order", '$.maker_tokens')), '$[0]')) as maker_length
    FROM
        (SELECT
            evt_index, evt_tx_hash, evt_block_time, ROW_NUMBER() OVER (PARTITION BY evt_tx_hash ORDER BY evt_index) AS row_num
         FROM {{ source('bebop_v3_arbitrum', 'BebopAggregationContract_evt_AggregateOrderExecuted') }}
         UNION ALL
         SELECT
            evt_index, evt_tx_hash, evt_block_time, ROW_NUMBER() OVER (PARTITION BY evt_tx_hash ORDER BY evt_index) AS row_num
         FROM {{ source('bebop_v4_arbitrum', 'BebopSettlement_evt_AggregateOrderExecuted') }}) evt
    LEFT JOIN
        (SELECT
            call_success, call_block_time, call_block_number, call_tx_hash, contract_address, "order",
            ROW_NUMBER() OVER (PARTITION BY call_tx_hash ORDER BY call_block_number) AS row_num
        FROM {{ source('bebop_v3_arbitrum', 'BebopAggregationContract_call_SettleAggregateOrder') }}
        UNION ALL
        SELECT
            call_success, call_block_time, call_block_number, call_tx_hash, contract_address, "order",
            ROW_NUMBER() OVER (PARTITION BY call_tx_hash ORDER BY call_block_number) AS row_num
        FROM {{ source('bebop_v4_arbitrum', 'BebopSettlement_call_SettleAggregateOrder') }}
        UNION ALL
        SELECT
            call_success, call_block_time, call_block_number, call_tx_hash, contract_address, "order",
            ROW_NUMBER() OVER (PARTITION BY call_tx_hash ORDER BY call_block_number) AS row_num
        FROM {{ source('bebop_v4_arbitrum', 'BebopSettlement_call_SettleAggregateOrderWithTakerPermits') }}
        ) ex
        ON ex.call_tx_hash = evt.evt_tx_hash and ex.row_num = evt.row_num
    WHERE ex.call_success = TRUE
    {% if is_incremental() %}
    AND evt.evt_block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
),

unnested_array_taker AS (
    SELECT
        block_time,
        block_number,
        tx_hash,
        evt_index,
        contract_address,
        expiry,
        taker_address,
        maker_address,
        taker_tokens_json,
        maker_tokens_json,
        taker_amounts_json,
        maker_amounts_json,
        taker_length,
        maker_length,
        element_at(CAST(json_extract(taker_tokens_json, '$[0]') AS ARRAY<VARCHAR>), sequence_number) AS taker_token_address,
        element_at(CAST(json_extract(taker_amounts_json, '$[0]') AS ARRAY<VARCHAR>), sequence_number) AS taker_token_amounts, 
        sequence_number - 1 AS taker_index
    FROM bebop_raw_data
    CROSS JOIN UNNEST(sequence(1, json_array_length(json_extract(taker_tokens_json, '$[0]')))) AS t(sequence_number)
), 

unnested_array_maker AS (
    SELECT
        block_time,
        block_number,
        tx_hash,
        evt_index,
        contract_address,
        expiry,
        taker_address,
        maker_address,
        taker_tokens_json,
        maker_tokens_json,
        taker_amounts_json,
        maker_amounts_json,
        taker_token_address,
        taker_token_amounts,
        taker_index,
        taker_length,
        maker_length,
        element_at(CAST(json_extract(maker_tokens_json, '$[0]') AS ARRAY<VARCHAR>), sequence_number) AS maker_token_address,
        element_at(CAST(json_extract(maker_amounts_json, '$[0]') AS ARRAY<VARCHAR>), sequence_number) AS maker_token_amounts, 
        sequence_number - 1 AS maker_index
    FROM unnested_array_taker
    CROSS JOIN UNNEST(sequence(1, json_array_length(json_extract(maker_tokens_json, '$[0]')))) AS t(sequence_number)
), 

simple_trades as (
    SELECT 
      block_time, 
      block_number,
      contract_address, 
      tx_hash, 
      evt_index,
      taker_address, 
      maker_address,
      taker_length,
      maker_length,
      CASE 
        WHEN taker_length = 1 AND maker_length > 1 THEN CAST(array[taker_index, maker_index] as array<bigint>)
        WHEN maker_length = 1 AND taker_length > 1 THEN CAST(array[maker_index, taker_index] as array<bigint>)
      ELSE CAST(array[taker_index, maker_index] as array<bigint>)
      END as trace_address,
      CASE 
        WHEN taker_length = 1 AND maker_length > 1 THEN 'Multi-Buy' -- inverted 
        WHEN maker_length = 1 AND taker_length > 1 THEN 'Multi-Sell' -- inverted, noted below... 
      ELSE 'Simple-Swap'
      END as trade_type,
      from_hex(maker_token_address) as token_bought_address, -- for some weird reason, this is inverted, based on the spark version of this query & also on arbiscan
      from_hex(taker_token_address) as token_sold_address, -- noted above 
      CAST(maker_token_amounts as UINT256) as token_bought_amount_raw,
      CAST(maker_token_amounts as double) as token_bought_amount,
      CAST(taker_token_amounts as UINT256) as token_sold_amount_raw,
      CAST(taker_token_amounts as double) as token_sold_amount
    FROM 
    unnested_array_maker
    WHERE maker_token_address IS NOT NULL 
    AND taker_token_address IS NOT NULL 
)

SELECT
  'arbitrum' AS blockchain,
  'bebop' AS project,
  '2' AS version,
  CAST(date_trunc('DAY', t.block_time) AS date) AS block_date,
  CAST(date_trunc('MONTH', t.block_time) AS date) AS block_month,
  t.block_time AS block_time,
  t.trade_type,
  t_bought.symbol AS token_bought_symbol,
  t_sold.symbol AS token_sold_symbol,
  CASE
    WHEN lower(t_bought.symbol) > lower(t_sold.symbol) THEN concat(t_sold.symbol, '-', t_bought.symbol)
    ELSE concat(t_bought.symbol, '-', t_sold.symbol)
  END AS token_pair,
  t.token_bought_amount / power(10, coalesce(t_bought.decimals, 0)) AS token_bought_amount,
  t.token_sold_amount / power(10, coalesce(t_sold.decimals, 0)) AS token_sold_amount,
  t.token_bought_amount_raw,
  t.token_sold_amount_raw,
  CASE 
    WHEN t.trade_type = 'Multi-Buy' THEN COALESCE(
        (t.token_bought_amount / power(10, t_bought.decimals)) * p_bought.price,
        (t.token_sold_amount / power(10, t_sold.decimals)) * p_sold.price / maker_length
    )
    WHEN t.trade_type = 'Multi-Sell' THEN COALESCE(
        (t.token_sold_amount / power(10, t_sold.decimals)) * p_sold.price,
        (t.token_bought_amount / power(10, t_bought.decimals)) * p_bought.price / taker_length
    )
    ELSE COALESCE(
        (t.token_bought_amount / power(10, t_bought.decimals)) * p_bought.price,
        (t.token_sold_amount / power(10, t_sold.decimals)) * p_sold.price
    ) 
    END as amount_usd, -- when there's a Multi-trade, the usd value of the multi tokens traded is used as the amount_usd 
    t.token_bought_address,
    t.token_sold_address,
    t.taker_address AS taker,
    t.contract_address AS maker,
    t.contract_address AS project_contract_address,
    t.tx_hash,
    tx."from" tx_from,
    tx.to AS tx_to,
    t.trace_address,
    t.evt_index
FROM
simple_trades t
INNER JOIN 
{{ source('arbitrum', 'transactions')}} tx
    ON t.tx_hash = tx.hash
    {% if not is_incremental() %}
    AND tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
  LEFT JOIN 
  {{ source('tokens', 'erc20') }} t_bought 
    ON t_bought.contract_address = t.token_bought_address
    AND t_bought.blockchain = 'arbitrum'
  LEFT JOIN 
  {{ source('tokens', 'erc20') }} t_sold
    ON t_sold.contract_address = t.token_sold_address
    AND t_sold.blockchain = 'arbitrum'
  LEFT JOIN 
  {{ source('prices', 'usd') }} p_bought 
    ON p_bought.minute = date_trunc('minute', t.block_time)
    AND p_bought.contract_address = t.token_bought_address
    AND p_bought.blockchain = 'arbitrum'
  {% if not is_incremental() %}
  AND p_bought.minute >= TIMESTAMP '{{project_start_date}}'
  {% endif %}
  {% if is_incremental() %}
  AND p_bought.minute >= date_trunc('day', now() - interval '7' Day)
  {% endif %}
  LEFT JOIN 
  {{ source('prices', 'usd') }} p_sold 
    ON p_sold.minute = date_trunc('minute', t.block_time)
    AND p_sold.contract_address = t.token_sold_address
    AND p_sold.blockchain = 'arbitrum'
  {% if not is_incremental() %}
  AND p_sold.minute >= TIMESTAMP '{{project_start_date}}'
  {% endif %}
  {% if is_incremental() %}
  AND p_sold.minute >= date_trunc('day', now() - interval '7' Day)
  {% endif %}