{{ config(
    tags=['prod_exclude'],
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                "project",
                                "odos",
                                \'["Henrystats"]\') }}'
    )
}}


{% set project_start_date = '2022-11-29' %}

WITH dexs_raw_tokens_bought AS (
  SELECT
    evt_tx_hash,
    evt_block_time,
    idx,
    CASE
      WHEN output:tokenAddress = '0x0000000000000000000000000000000000000000' THEN '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7' -- WAVAX
      ELSE output:tokenAddress
    END AS token_bought_address,
    amountsOut
    FROM (
      SELECT
        evt_tx_hash,
        evt_block_time,
        POSEXPLODE_OUTER(outputs) AS (idx, output),
        amountsOut
      FROM {{ source('odos_avalanche_c', 'OdosRouter_evt_Swapped') }}
    ) swaps
),

dexs_raw_tokens_bought_agg AS (
  SELECT
    evt_tx_hash,
    ARRAY_AGG(
      STRUCT(
        token_bought_address AS token,
        amountsOut[idx] AS amount_raw,
        erc20a.symbol as token_bought_symbol,
        amountsOut[idx] / POWER(10, erc20a.decimals) AS amount,
        (amountsOut[idx] / POWER(10, p_bought.decimals)) * p_bought.price AS amount_usd
      )
    ) AS tokens_bought,
    SUM((amountsOut[idx] / POWER(10, p_bought.decimals)) * p_bought.price) AS amount_usd
    FROM dexs_raw_tokens_bought AS dexs_raw_tokens_bought
    LEFT JOIN {{ ref('tokens_erc20') }} erc20a
          ON erc20a.contract_address = dexs_raw_tokens_bought.token_bought_address
          AND erc20a.blockchain = 'avalanche_c'
    LEFT JOIN {{ source('prices', 'usd') }} p_bought
          ON p_bought.minute = DATE_TRUNC('MINUTE', dexs_raw_tokens_bought.evt_block_time)
          AND p_bought.contract_address = dexs_raw_tokens_bought.token_bought_address
          AND p_bought.blockchain = 'avalanche_c'
          {% if not is_incremental() %}
          AND p_bought.minute >= '{{project_start_date}}'
          {% endif %}
          {% if is_incremental%}
          AND p_bought.minute >= DATE_TRUNC("DAY", NOW() - INTERVAL '1 WEEK')
          {% endif %}
    GROUP BY evt_tx_hash
),

dexs_raw_tokens_sold AS (
  SELECT
    evt_tx_hash,
    evt_block_time,
    idx,
    CASE
      WHEN token = '0x0000000000000000000000000000000000000000' THEN '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7' -- WAVAX
      ELSE token
    END AS token_sold_address,
    amountsIn
    FROM (
      SELECT
        evt_tx_hash,
        evt_block_time,
        POSEXPLODE_OUTER(tokensIn) AS (idx, token),
        amountsIn
      FROM {{ source('odos_avalanche_c', 'OdosRouter_evt_Swapped') }}
    ) swaps
),

dexs_raw_tokens_sold_agg AS (
  SELECT
    evt_tx_hash,
    ARRAY_AGG(
      STRUCT(
        token_sold_address AS token,
        amountsIn[idx] AS amount_raw,
        erc20a.symbol as token_sold_symbol,
        amountsIn[idx] / POWER(10, erc20a.decimals) AS amount,
        (amountsIn[idx] / POWER(10, p_sold.decimals)) * p_sold.price AS amount_usd
      )
    ) AS tokens_sold,
    SUM((amountsIn[idx] / POWER(10, p_sold.decimals)) * p_sold.price) AS amount_usd
    FROM dexs_raw_tokens_sold
    LEFT JOIN {{ ref('tokens_erc20') }} erc20a
           ON erc20a.contract_address = dexs_raw_tokens_sold.token_sold_address
        AND erc20a.blockchain = 'avalanche_c'
    LEFT JOIN {{ source('prices', 'usd') }} p_sold
        ON p_sold.minute = DATE_TRUNC('MINUTE', dexs_raw_tokens_sold.evt_block_time)
        AND p_sold.contract_address = dexs_raw_tokens_sold.token_sold_address
        AND p_sold.blockchain = 'avalanche_c'
        AND p_sold.minute >= '2022-11-29'
    GROUP BY evt_tx_hash
),

dexs_raw as (
  SELECT
    s.evt_block_time AS block_time,
    '' AS maker,
    tokens_bought_agg.tokens_bought,
    tokens_sold_agg.tokens_sold,
    COALESCE(CAST(NULL as double), tokens_bought_agg.amount_usd, tokens_sold_agg.amount_usd) AS amount_usd,
    s.contract_address as project_contract_address,
    s.evt_tx_hash as tx_hash,
    CAST(ARRAY() as ARRAY<bigint>) AS trace_address,
    s.evt_index,
    s.sender AS taker
  FROM {{ source('odos_avalanche_c', 'OdosRouter_evt_Swapped') }} AS s
  LEFT JOIN dexs_raw_tokens_bought_agg AS tokens_bought_agg
         ON tokens_bought_agg.evt_tx_hash = s.evt_tx_hash
  LEFT JOIN dexs_raw_tokens_sold_agg AS tokens_sold_agg
         ON tokens_sold_agg.evt_tx_hash = s.evt_tx_hash

)

SELECT
    'avalanche_c' as blockchain,
    'odos' as project,
    '1' as version,
    TRY_CAST(date_trunc('DAY', dexs.block_time) as date) as block_date,
    dexs.block_time,
    dexs.amount_usd,
    dexs.tokens_bought,
    dexs.tokens_sold,
    COALESCE(dexs.taker, tx.from) as taker,  -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    tx.from as tx_from,
    tx.to AS tx_to,
    dexs.trace_address,
    dexs.evt_index,

    -- to remove, adding this temporary to allow CI to pass
    NULL AS token_
    NULL AS token_bought_symbol,
    NULL AS token_sold_symbol,
    NULL AS token_pair,
    NULL AS token_bought_amount,
    NULL AS token_sold_amount,,
    NULL AS token_bought_amount_raw,
    NULL AS token_sold_amount_raw,
    NULL AS amount_usd,
    NULL AS token_bought_address,
    NULL AS token_sold_address

FROM dexs_raw AS dexs
INNER JOIN {{ source('avalanche_c', 'transactions') }} tx
    ON tx.hash = dexs.tx_hash
    {% if not is_incremental() %}
    AND tx.block_time >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= DATE_TRUNC("DAY", NOW() - INTERVAL '1 WEEK')
    {% endif %}
