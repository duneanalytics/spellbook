{{ config(
	tags=['legacy'],
	
    alias = alias('borrow', legacy_model=True),
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_hash', 'pool_pair', 'maturity', 'strike'],
    post_hook='{{ expose_spells(\'["polygon"]\',
                                "project",
                                "timeswap",
                                \'["raveena15, varunhawk19"]\') }}'
    )
}}




SELECT
    b.call_tx_hash as transaction_hash,
    b.call_block_time as time,
    CAST(get_json_object(b.param, '$.isToken') AS BOOLEAN) AS token_0,
    'borrow' as transaction_type,
    get_json_object(b.param, '$.maturity') AS maturity,
    get_json_object(b.param, '$.strike') AS strike,
    i.pool_pair as pool_pair,
    i.chain as chain,
    tx.from as user,
    CAST(
        CASE
            WHEN CAST(get_json_object(b.param, '$.isToken') AS BOOLEAN) = true
            THEN CAST(get_json_object(b.param, '$.tokenAmount') AS DOUBLE) / power(10,i.token0_decimals)
            ELSE CAST(get_json_object(b.param, '$.tokenAmount') AS DOUBLE) / power(10,i.token1_decimals)
        END as DOUBLE
    ) as token_amount,
    CAST(
        CASE
            WHEN CAST(get_json_object(b.param, '$.isToken') AS BOOLEAN) = true
            THEN CAST(get_json_object(b.param, '$.tokenAmount') AS DOUBLE) / power(10,i.token0_decimals) * p.price
            ELSE CAST(get_json_object(b.param, '$.tokenAmount') AS DOUBLE) / power(10,i.token1_decimals) * p.price
        END as DOUBLE
    ) as usd_amount
    FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryUniswapV3BorrowGivenPrincipal_call_borrowGivenPrincipal') }} b
    JOIN {{ ref('timeswap_polygon_pools_legacy') }} i ON CAST(maturity as VARCHAR(100)) = i.maturity and cast(strike as VARCHAR(100)) = i.strike
    JOIN {{ source('polygon', 'transactions') }} tx
    on b.call_tx_hash = tx.hash
    {% if is_incremental() %}
    and tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    JOIN {{ source('prices', 'usd') }} p
    ON p.symbol=i.token0_symbol
    and p.blockchain = 'polygon'
    and b.call_success = true
    and CAST(get_json_object(b.param, '$.isToken') AS BOOLEAN) = true
    AND p.minute = date_trunc('minute',b.call_block_time)
    {% if is_incremental() %}
    AND p.minute >= date_trunc("day", now() - interval '1 week')
    WHERE b.call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}


UNION  


SELECT
    b.call_tx_hash as transaction_hash,
    b.call_block_time as time,
    CAST(get_json_object(b.param, '$.isToken') AS BOOLEAN) AS token_0,
    'borrow' as transaction_type,
    get_json_object(b.param, '$.maturity') AS maturity,
    get_json_object(b.param, '$.strike') AS strike,
    i.pool_pair as pool_pair,
    i.chain as chain,
    tx.from as user,
    CAST(
        CASE
            WHEN CAST(get_json_object(b.param, '$.isToken') AS BOOLEAN) = true
            THEN CAST(get_json_object(b.param, '$.tokenAmount') AS DOUBLE) / power(10,i.token0_decimals)
            ELSE CAST(get_json_object(b.param, '$.tokenAmount') AS DOUBLE) / power(10,i.token1_decimals)
        END as DOUBLE
    ) as token_amount,
    CAST(
        CASE
            WHEN CAST(get_json_object(b.param, '$.isToken') AS BOOLEAN) = true
            THEN CAST(get_json_object(b.param, '$.tokenAmount') AS DOUBLE) / power(10,i.token0_decimals) * p.price
            ELSE CAST(get_json_object(b.param, '$.tokenAmount') AS DOUBLE) / power(10,i.token1_decimals) * p.price
        END as DOUBLE
    ) as usd_amount
    FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryUniswapV3BorrowGivenPrincipal_call_borrowGivenPrincipal') }} b
    JOIN {{ ref('timeswap_polygon_pools_legacy') }} i ON CAST(maturity as VARCHAR(100)) = i.maturity and cast(strike as VARCHAR(100)) = i.strike
    JOIN {{ source('polygon', 'transactions') }} tx
    on b.call_tx_hash = tx.hash
    {% if is_incremental() %}
    and tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    JOIN {{ source('prices', 'usd') }} p
    ON p.symbol=i.token1_symbol
    and p.blockchain = 'polygon'
    and b.call_success = true
    and CAST(get_json_object(b.param, '$.isToken') AS BOOLEAN) = false
    AND p.minute = date_trunc('minute',b.call_block_time)
    {% if is_incremental() %}
    AND p.minute >= date_trunc("day", now() - interval '1 week')
    WHERE b.call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}


UNION


SELECT
  b.evt_tx_hash as transaction_hash,
  b.evt_block_time as time,
  b.isToken0 as token_0,
  'borrow' as transaction_type,
  b.maturity as maturity,
  b.strike as strike,
  i.pool_pair as pool_pair,
  i.chain as chain,
  tx.from as user,
  CAST(
    CASE
      WHEN CAST(b.isToken0 AS BOOLEAN) = true THEN CAST(b.tokenAmount AS DOUBLE) / power(10,i.token0_decimals)
      ELSE CAST(b.tokenAmount AS DOUBLE) / power(10,i.token1_decimals)
    END as DOUBLE
  ) as token_amount,
  CAST(
    CASE
      WHEN CAST(b.isToken0 AS BOOLEAN) = true THEN CAST(b.tokenAmount AS DOUBLE) / power(10,i.token0_decimals) * p.price
      ELSE CAST(b.tokenAmount AS DOUBLE) / power(10,i.token1_decimals) * p.price
    END as DOUBLE
  ) as usd_amount
FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryUniswapV3BorrowGivenPrincipal_evt_BorrowGivenPrincipal') }} b
JOIN {{ ref('timeswap_polygon_pools_legacy') }} i
  ON CAST(b.maturity as VARCHAR(100)) = i.maturity
  and cast(b.strike as VARCHAR(100)) = i.strike
JOIN {{ source('polygon', 'transactions') }} tx
    on b.evt_tx_hash = tx.hash
    {% if is_incremental() %}
    and tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
JOIN {{ source('prices', 'usd') }} p
  ON p.symbol=i.token0_symbol
  and p.blockchain = 'polygon'
  and b.isToken0 = true
  AND p.minute = date_trunc('minute',b.evt_block_time)
  {% if is_incremental() %}
  AND p.minute >= date_trunc("day", now() - interval '1 week')
WHERE b.evt_block_time >= date_trunc("day", now() - interval '1 week')
  {% endif %}


UNION
 
SELECT
  b.evt_tx_hash as transaction_hash,
  b.evt_block_time as time,
  b.isToken0 as token_0,
  'borrow' as transaction_type,
  b.maturity as maturity,
  b.strike as strike,
  i.pool_pair as pool_pair,
  i.chain as chain,
  tx.from as user,
  CAST(
    CASE
      WHEN CAST(b.isToken0 AS BOOLEAN) = true THEN CAST(b.tokenAmount AS DOUBLE) / power(10,i.token0_decimals)
      ELSE CAST(b.tokenAmount AS DOUBLE) / power(10,i.token1_decimals)
    END as DOUBLE
  ) as token_amount,
  CAST(
    CASE
      WHEN CAST(b.isToken0 AS BOOLEAN) = true THEN CAST(b.tokenAmount AS DOUBLE) / power(10,i.token0_decimals) * p.price
      ELSE CAST(b.tokenAmount AS DOUBLE) / power(10,i.token1_decimals) * p.price
    END as DOUBLE
  ) as usd_Amount
FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryUniswapV3BorrowGivenPrincipal_evt_BorrowGivenPrincipal') }} b
JOIN {{ ref('timeswap_polygon_pools_legacy') }} i
  ON CAST(b.maturity as VARCHAR(100)) = i.maturity
  and cast(b.strike as VARCHAR(100)) = i.strike
JOIN {{ source('polygon', 'transactions') }} tx
    on b.evt_tx_hash = tx.hash
    {% if is_incremental() %}
    and tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
JOIN {{ source('prices', 'usd') }} p
  ON p.symbol=i.token1_symbol
  and p.blockchain = 'polygon'
  and b.isToken0 = false
  AND p.minute = date_trunc('minute',b.evt_block_time)
  {% if is_incremental() %}
  AND p.minute >= date_trunc("day", now() - interval '1 week')
WHERE b.evt_block_time >= date_trunc("day", now() - interval '1 week')
  {% endif %}




UNION


SELECT
  b.evt_tx_hash as transaction_hash,
  b.evt_block_time as time,
  b.isToken0 as token_0,
  'borrow' as transaction_type,
  b.maturity as maturity,
  b.strike as strike,
  i.pool_pair as pool_pair,
  i.chain as chain,
  tx.from as user,
  CAST(
    CASE
      WHEN CAST(b.isToken0 AS BOOLEAN) = true THEN CAST(b.tokenAmount AS DOUBLE) / power(10,i.token0_decimals)
      ELSE CAST(b.tokenAmount AS DOUBLE) / power(10,i.token1_decimals)
    END as DOUBLE
  ) as token_amount,
  CAST(
    CASE
      WHEN CAST(b.isToken0 AS BOOLEAN) = true THEN CAST(b.tokenAmount AS DOUBLE) / power(10,i.token0_decimals) * p.price
      ELSE CAST(b.tokenAmount AS DOUBLE) / power(10,i.token1_decimals) * p.price
    END as DOUBLE
  ) as usd_amount
FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryNoDexBorrowGivenPrincipal_evt_BorrowGivenPrincipal') }} b
JOIN {{ ref('timeswap_polygon_pools_legacy') }} i
  ON CAST(b.maturity as VARCHAR(100)) = i.maturity
  and cast(b.strike as VARCHAR(100)) = i.strike
JOIN {{ source('polygon', 'transactions') }} tx
    on b.evt_tx_hash = tx.hash
    {% if is_incremental() %}
    and tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
JOIN {{ source('prices', 'usd') }} p
  ON p.symbol=i.token0_symbol
  and p.blockchain = 'polygon'
  and b.isToken0 = true
  AND p.minute = date_trunc('minute',b.evt_block_time)
  {% if is_incremental() %}
  AND p.minute >= date_trunc("day", now() - interval '1 week')
WHERE b.evt_block_time >= date_trunc("day", now() - interval '1 week')
  {% endif %}


UNION
 
SELECT
  b.evt_tx_hash as transaction_hash,
  b.evt_block_time as time,
  b.isToken0 as token_0,
  'borrow' as transaction_type,
  b.maturity as maturity,
  b.strike as strike,
  i.pool_pair as pool_pair,
  i.chain as chain,
  tx.from as user,
  CAST(
    CASE
      WHEN CAST(b.isToken0 AS BOOLEAN) = true THEN CAST(b.tokenAmount AS DOUBLE) / power(10,i.token0_decimals)
      ELSE CAST(b.tokenAmount AS DOUBLE) / power(10,i.token1_decimals)
    END as DOUBLE
  ) as token_amount,
  CAST(
    CASE
      WHEN CAST(b.isToken0 AS BOOLEAN) = true THEN CAST(b.tokenAmount AS DOUBLE) / power(10,i.token0_decimals) * p.price
      ELSE CAST(b.tokenAmount AS DOUBLE) / power(10,i.token1_decimals) * p.price
    END as DOUBLE
  ) as usd_amount
FROM {{ source('timeswap_polygon', 'TimeswapV2PeripheryNoDexBorrowGivenPrincipal_evt_BorrowGivenPrincipal') }} b
JOIN {{ ref('timeswap_polygon_pools_legacy') }} i
  ON CAST(b.maturity as VARCHAR(100)) = i.maturity
  and cast(b.strike as VARCHAR(100)) = i.strike
JOIN {{ source('polygon', 'transactions') }} tx
    on b.evt_tx_hash = tx.hash
    {% if is_incremental() %}
    and tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
JOIN {{ source('prices', 'usd') }} p
  ON p.symbol=i.token1_symbol
  and p.blockchain = 'polygon'
  and b.isToken0 = false
  AND p.minute = date_trunc('minute',b.evt_block_time)
  {% if is_incremental() %}
  AND p.minute >= date_trunc("day", now() - interval '1 week')
WHERE b.evt_block_time >= date_trunc("day", now() - interval '1 week')
  {% endif %}
