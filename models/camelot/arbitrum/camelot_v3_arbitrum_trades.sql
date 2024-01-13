{{  config(
    schema = 'camelot_v3_arbitrum',
    alias = 'trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'])
}}


{% set project_start_date = '2022-06-14' %}
{% set blockchain = 'arbitrum' %}

WITH dexs AS (
  SELECT
    swaps.evt_block_time AS block_time,
    swaps.recipient AS taker,
    CAST(NULL as VARBINARY) AS maker,
    CASE
      WHEN swaps.amount0 < INT256 '0' THEN abs(swaps.amount0)
      ELSE abs(swaps.amount1)
    END AS token_bought_amount_raw,
    CASE
      WHEN swaps.amount0 < INT256 '0' THEN abs(swaps.amount1)
      ELSE abs(swaps.amount0)
    END AS token_sold_amount_raw,
    NULL AS amount_usd,
    CASE
      WHEN amount0 < INT256 '0' THEN pools.token0
      ELSE pools.token1
    END AS token_bought_address,
    CASE
      WHEN amount0 < INT256 '0' THEN pools.token1
      ELSE pools.token0
    END AS token_sold_address,
    swaps.contract_address AS project_contract_address,
    swaps.evt_tx_hash AS tx_hash,
    swaps.evt_index
  FROM 
    {{ source('camelot_v3_arbitrum', 'AlgebraPool_evt_Swap') }} AS swaps
  INNER JOIN 
    {{ source('camelot_v3_arbitrum', 'AlgebraFactory_evt_Pool') }} AS pools
      ON swaps.contract_address = pools.pool
  {% if is_incremental() %}
  WHERE {{ incremental_predicate('swaps.evt_block_time') }}
  {% endif %}
)

SELECT
  'arbitrum' AS blockchain,
  'camelot' AS project,
  '3' AS version,
  CAST(date_trunc('month', dexs.block_time) AS date) AS block_month,
  CAST(date_trunc('DAY', dexs.block_time) AS date) AS block_date,
  dexs.block_time,
  erc20a.symbol AS token_bought_symbol,
  erc20b.symbol AS token_sold_symbol,
  CASE
    WHEN LOWER(erc20a.symbol) > LOWER(erc20b.symbol) THEN CONCAT(erc20b.symbol, '-', erc20a.symbol)
    ELSE CONCAT(erc20a.symbol, '-', erc20b.symbol)
  END AS token_pair,
  dexs.token_bought_amount_raw / POWER(10, erc20a.decimals) AS token_bought_amount,
  dexs.token_sold_amount_raw / POWER(10, erc20b.decimals) AS token_sold_amount,
  CAST(dexs.token_bought_amount_raw AS UINT256) AS token_bought_amount_raw,
  CAST(dexs.token_sold_amount_raw AS UINT256) AS token_sold_amount_raw,
  COALESCE(
    dexs.amount_usd,
    (
      dexs.token_bought_amount_raw / power(10, p_bought.decimals)
    ) * p_bought.price,
    (
      dexs.token_sold_amount_raw / power(10, p_sold.decimals)
    ) * p_sold.price
  ) AS amount_usd,
  dexs.token_bought_address,
  dexs.token_sold_address,
  coalesce(dexs.taker, tx."from") AS taker,
  dexs.maker,
  dexs.project_contract_address,
  dexs.tx_hash,
  tx."from" AS tx_from,
  tx.to AS tx_to,
  dexs.evt_index
FROM 
  dexs
INNER JOIN {{ source('arbitrum', 'transactions') }} tx
  ON tx.hash = dexs.tx_hash
  {% if not is_incremental() %}
  AND tx.block_time >= TIMESTAMP '{{project_start_date}}'
  {% endif %}
  {% if is_incremental() %}
  AND {{ incremental_predicate('tx.block_time') }}
  {% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} erc20a
  ON erc20a.contract_address = dexs.token_bought_address
  AND erc20a.blockchain = '{{blockchain}}'
LEFT JOIN {{ source('tokens', 'erc20') }} erc20b
  ON erc20b.contract_address = dexs.token_sold_address
  AND erc20b.blockchain = '{{blockchain}}'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
  ON p_bought.minute = DATE_TRUNC('minute', dexs.block_time)
  AND p_bought.contract_address = dexs.token_bought_address
  AND p_bought.blockchain = '{{blockchain}}'
  {% if not is_incremental() %}
  AND p_bought.minute >= TIMESTAMP '{{project_start_date}}'
  {% endif %}
  {% if is_incremental() %}
  AND {{ incremental_predicate('p_bought.minute') }}
  {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
  ON p_sold.minute = DATE_TRUNC('minute', dexs.block_time)
  AND p_sold.contract_address = dexs.token_sold_address
  AND p_sold.blockchain = '{{blockchain}}'
  {% if not is_incremental() %}
  AND p_sold.minute >= TIMESTAMP '{{project_start_date}}'
  {% endif %}
  {% if is_incremental() %}
  AND {{ incremental_predicate('p_sold.minute') }}
  {% endif %}

