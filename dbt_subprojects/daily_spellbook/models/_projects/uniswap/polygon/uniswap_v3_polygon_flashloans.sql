{{ config(
     partition_by = ['block_month']
      , schema = 'uniswap_v3_polygon'
      , alias = 'flashloans'
      , materialized = 'incremental'
      , file_format = 'delta'
      , incremental_strategy = 'merge'
      , unique_key = ['tx_hash', 'evt_index']
      , post_hook='{{ expose_spells(\'["ethereum"]\',
                                  "project",
                                  "uniswap_v3",
                                  \'["hildobby"]\') }}'
  )
}}

WITH flashloans AS (
    SELECT f.evt_block_time AS block_time
    , f.evt_block_number AS block_number
    , CASE WHEN f.amount0 = UINT256 '0' THEN f.amount1 ELSE f.amount0 END AS amount_raw
    , f.evt_tx_hash AS tx_hash
    , f.evt_index
    , CASE WHEN f.amount0 = UINT256 '0' THEN f.paid1 ELSE f.paid0 END AS fee
    , CASE WHEN f.amount0 = UINT256 '0' THEN p.token1 ELSE p.token0 END AS currency_contract
    , CASE WHEN f.amount0 = UINT256 '0' THEN erc20b.symbol ELSE erc20a.symbol END AS currency_symbol
    , CASE WHEN f.amount0 = UINT256 '0' THEN erc20b.decimals ELSE erc20a.decimals END AS currency_decimals
    , f.contract_address
    FROM {{ source('uniswap_v3_polygon','UniswapV3Pool_evt_Flash') }} f
        INNER JOIN {{ source('uniswap_v3_polygon','Factory_evt_PoolCreated') }} p ON f.contract_address = p.pool
    LEFT JOIN {{ source('tokens_polygon', 'erc20') }} erc20a ON p.token0 = erc20a.contract_address
    LEFT JOIN {{ source('tokens_polygon', 'erc20') }} erc20b ON p.token1 = erc20b.contract_address
    WHERE f.evt_block_time > NOW() - interval '1' month
        {% if is_incremental() %}
        AND f.evt_block_time >= date_trunc('day', now() - interval '7' Day)
        {% endif %}
    )

SELECT 'polygon' AS blockchain
, 'uniswap' AS project
, '3' AS version
, CAST(date_trunc('Month', flash.block_time) as date) as block_month
, flash.block_time
, flash.block_number
, flash.amount_raw/POWER(10, flash.currency_decimals) AS amount
, pu.price*flash.amount_raw/POWER(10, flash.currency_decimals) AS amount_usd
, flash.tx_hash
, flash.evt_index
, flash.fee/POWER(10, flash.currency_decimals) AS fee
, flash.currency_contract
, flash.currency_symbol
, flash.contract_address
FROM flashloans flash
LEFT JOIN {{ source('prices','usd') }} pu ON pu.blockchain = 'polygon'  
    AND pu.contract_address = flash.currency_contract
    AND pu.minute = date_trunc('minute', flash.block_time)
    {% if is_incremental() %}
    AND pu.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}