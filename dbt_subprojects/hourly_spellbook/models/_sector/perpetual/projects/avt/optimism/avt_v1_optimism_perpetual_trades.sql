{{ config(
	
	schema = 'avt_v1_optimism',
	alias = 'perpetual_trades',
	partition_by = ['block_month'],
	materialized = 'incremental',
	file_format = 'delta',
	incremental_strategy = 'merge',
	unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index']
	)
}}

{%
set project_start_date = '2023-03-27' %}

WITH all_executed_positions AS (
SELECT block_time,
       block_number,
       index,
       contract_address,
       tx_hash,
       bytearray_substring(data, 45, 20)                       AS account,
       bytearray_substring(data, 77, 20)                       AS collateralToken,
       bytearray_substring(data, 109, 20)                      AS indexToken,
       varbinary_to_int256(bytearray_substring(data, 141, 20)) AS collateralDelta,
       varbinary_to_int256(bytearray_substring(data, 173, 20)) AS sizeDelta,
       (CASE
            WHEN varbinary_to_int256(bytearray_substring(data, 205, 20)) = 1 THEN true
            ELSE false
           END)                                                AS isLong,
       varbinary_to_int256(bytearray_substring(data, 237, 20)) AS price,
       'Open'                                                  AS trade_type
FROM {{ source('optimism', 'logs') }}

WHERE contract_address = 0x24ee37267842a525c66fe37cd0da749150e89866
  AND topic0 = 0x2fe68525253654c21998f35787a8d0f361905ef647c854092430ab65f2f15022
  AND tx_hash IN ( SELECT evt_tx_hash FROM {{ source('avt_optimism', 'PositionRouter_evt_ExecuteIncreasePosition') }} )
    {% if not is_incremental() %}
  AND block_time >= DATE '{{project_start_date}}'
    {% else %}
  AND {{ incremental_predicate('block_time') }}
    {% endif %}

UNION ALL

SELECT 
    block_time,
    block_number,
    index,
    contract_address,
    tx_hash,
    bytearray_substring(data,45,20) AS account,
    bytearray_substring(data,77,20) AS collateralToken,
    bytearray_substring(data,109,20) AS indexToken,
    varbinary_to_int256(bytearray_substring(data,141,20)) AS collateralDelta,
    varbinary_to_int256(bytearray_substring(data,173,20)) AS sizeDelta,
    (CASE
    WHEN varbinary_to_int256(bytearray_substring(data,205,20)) = 1 THEN true
    ELSE false
    END) AS isLong,
    varbinary_to_int256(bytearray_substring(data,237,20)) AS price,
    'Close' AS trade_type
FROM {{ source('optimism','logs') }}

WHERE contract_address = 0x24ee37267842a525c66fe37cd0da749150e89866
  AND topic0 = 0x93d75d64d1f84fc6f430a64fc578bdd4c1e090e90ea2d51773e626d19de56d30
  AND tx_hash IN ( SELECT evt_tx_hash FROM {{ source('avt_optimism', 'PositionRouter_evt_ExecuteDecreasePosition') }} )
    {% if not is_incremental() %}
  AND block_time >= DATE '{{project_start_date}}'
    {% else %}
  AND {{ incremental_predicate('block_time') }}
    {% endif %}
),
margin_fees_info AS (
SELECT block_time,
       block_number,
       index,
       tx_hash,
       varbinary_to_int256(bytearray_substring(data, 45, 20))             AS feeUsd,
       LEAD(index, 1, 1000000) OVER (PARTITION BY tx_hash ORDER BY index) AS next_index
FROM {{ source('optimism', 'logs') }}
WHERE topic0 = 0x5d0c0019d3d45fadeb74eff9d2c9924d146d000ac6bcf3c28bf0ac3c9baa011a
  AND contract_address = 0x24ee37267842a525c66fe37cd0da749150e89866
  {% if not is_incremental() %}
  AND block_time >= DATE '{{project_start_date}}'
  {% else %}
  AND {{ incremental_predicate('block_time') }}
  {% endif %}
),

complete_perp_tx AS (
SELECT *, index_token || '/USD' AS market
FROM (SELECT event.*,
             tokens1.symbol AS underlying_asset,
             (CASE
                  WHEN tokens.symbol = 'WETH' THEN 'ETH'
                  WHEN tokens.symbol = 'WBTC' THEN 'BTC'
                  ELSE tokens.symbol
                 END
                 )          AS index_token,
             trx."from",
             trx.to,
             fees.feeUsd    AS margin_fee
      FROM all_executed_positions event
      INNER JOIN {{ source('optimism', 'transactions') }} trx
      ON event.tx_hash = trx.hash
          {% if not is_incremental() %}
          AND event.block_time >= DATE '{{project_start_date}}'
          {% else %}
          AND {{ incremental_predicate('event.block_time') }}
          {% endif %}
      INNER JOIN margin_fees_info fees
          ON event.tx_hash = fees.tx_hash
          AND event.index > fees.index
          AND event.index < fees.next_index
      INNER JOIN {{ source('tokens', 'erc20') }} tokens
          ON event.indexToken = tokens.contract_address
          AND tokens.blockchain = 'optimism'
      INNER JOIN {{ source('tokens', 'erc20') }} tokens1
          ON event.collateralToken = tokens1.contract_address
          AND tokens1.blockchain = 'optimism'
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18)

)

SELECT 'optimism'                                    AS blockchain
     , CAST(date_trunc('DAY', block_time) AS date)   AS block_date
     , CAST(date_trunc('MONTH', block_time) AS date) AS block_month
     , block_time
     , CAST(NULL AS VARCHAR)                         AS virtual_asset
     , underlying_asset
     , market
     , contract_address                              AS market_address
     , CAST(sizeDelta / 1e30 AS DOUBLE)              AS volume_usd
     , CAST(margin_fee / 1e30 AS DOUBLE)                fee_usd
     , CAST(collateralDelta / 1e30 AS DOUBLE)        AS margin_usd
     , (CASE
            WHEN isLong = false AND trade_type = 'Open' THEN 'Open Short'
            WHEN isLong = true AND trade_type = 'Open' THEN 'Open Long'
            WHEN isLong = false AND trade_type = 'Close' THEN 'Close Short'
            WHEN isLong = true AND trade_type = 'Close' THEN 'Close Long'
    END
    )                                                AS trade
     , 'AVT'                                         AS project
     , 'v1'                                          AS version
     , 'AVT'                                         AS frontend
     , account                                       AS trader
     , sizeDelta                                     AS volume_raw
     , tx_hash                                       AS tx_hash
     , "from"                                        AS tx_from
     , to AS tx_to
     , index AS evt_index
FROM complete_perp_tx

