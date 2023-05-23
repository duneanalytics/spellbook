{{ config(
      schema = 'uniswap_v3_bnb'
      , alias='flashloans'
      , materialized = 'incremental'
      , file_format = 'delta'
      , incremental_strategy = 'merge'
      , unique_key = ['tx_hash', 'evt_index']
      , post_hook='{{ expose_spells(\'["bnb"]\',
                                  "project",
                                  "uniswap_v3",
                                  \'["hildobby"]\') }}'
  )
}}

WITH flashloans AS (
    SELECT f.evt_block_time AS block_time
    , f.evt_block_number AS block_number
    , CASE WHEN f.amount0 = 0 THEN f.amount1 ELSE f.amount0 END AS amount_raw
    , f.evt_tx_hash AS tx_hash
    , f.evt_index
    , CASE WHEN f.amount0 = 0 THEN f.paid1 ELSE f.paid0 END AS fee
    , CASE WHEN f.amount0 = 0 THEN p.token1 ELSE p.token0 END AS currency_contract
    , CASE WHEN f.amount0 = 0 THEN bep20b.symbol ELSE bep20a.symbol END AS currency_symbol
    , CASE WHEN f.amount0 = 0 THEN bep20b.decimals ELSE bep20a.decimals END AS currency_decimals
    , f.contract_address
    FROM {{ source('uniswap_v3_bnb','Pair_evt_Flash') }} f
        INNER JOIN {{ source('uniswap_v3_bnb','UniswapV3Factory_evt_PoolCreated') }} p ON f.contract_address = p.pool
    LEFT JOIN {{ ref('tokens_bnb_bep20') }} bep20a ON p.token0 = bep20a.contract_address
    LEFT JOIN {{ ref('tokens_bnb_bep20') }} bep20b ON p.token1 = bep20b.contract_address
    WHERE f.evt_block_time > NOW() - interval '1' month
        {% if is_incremental() %}
        AND f.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    )

SELECT 'bnb' AS blockchain
, 'Uniswap' AS project
, '3' AS version
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
LEFT JOIN {{ source('prices','usd') }} pu ON pu.blockchain = 'bnb'  
    AND pu.contract_address = flash.currency_contract
    AND pu.minute = date_trunc('minute', flash.block_time)
