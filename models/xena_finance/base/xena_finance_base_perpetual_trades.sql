{{ config(
	schema = 'xena_finance_base',
	alias = 'perpetual_trades',
	partition_by = ['block_month'],
	materialized = 'incremental',
	file_format = 'delta',
	incremental_strategy = 'merge',
	unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index']
	)
}}

{%
set project_start_date = '2023-09-20' %}

WITH all_executed_positions AS (
SELECT evt_block_time,
       evt_block_number,
       evt_index,
       contract_address,
       evt_tx_hash,
       account,
       collateralToken,
       indexToken,
       collateralValue,
       sizeChanged,
       feeValue,
       (CASE
            WHEN side = 1 THEN true
            ELSE false
           END)                                                AS isLong,
       'Open'                                                  AS trade_type
FROM {{ source('xena_base', 'Pool_evt_IncreasePosition') }}
    {% if not is_incremental() %}
  WHERE evt_block_time >= DATE '{{project_start_date}}'
    {% else %}
  WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

UNION ALL

SELECT 
    evt_block_time,
    evt_block_number,
    evt_index,
    contract_address,
    evt_tx_hash,
    account,
    collateralToken,
    indexToken,
    collateralChanged,
    sizeChanged,
    feeValue,
    (CASE
    WHEN side = 1 THEN true
    ELSE false
    END) AS isLong,
    'Close' AS trade_type
FROM {{ source('xena_base','Pool_evt_DecreasePosition') }}
    {% if not is_incremental() %}
  WHERE evt_block_time >= DATE '{{project_start_date}}'
    {% else %}
  WHERE {{ incremental_predicate('evt_block_time') }}
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
             trx.to
      FROM all_executed_positions event
      INNER JOIN {{ source('base', 'transactions') }} trx
      ON event.evt_tx_hash = trx.hash
          {% if not is_incremental() %}
          AND event.evt_block_time >= DATE '{{project_start_date}}'
          {% else %}
          AND {{ incremental_predicate('event.evt_block_time') }}
          {% endif %}
      INNER JOIN {{ source('tokens', 'erc20') }} tokens
          ON event.indexToken = tokens.contract_address
          AND tokens.blockchain = 'base'
      INNER JOIN {{ source('tokens', 'erc20') }} tokens1
          ON event.collateralToken = tokens1.contract_address
          AND tokens1.blockchain = 'base'
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)

)

SELECT 'base'                                    AS blockchain
     , CAST(date_trunc('DAY', evt_block_time) AS date)   AS block_date
     , CAST(date_trunc('MONTH', evt_block_time) AS date) AS block_month
     , evt_block_time AS block_time
     , CAST(NULL AS VARCHAR)                         AS virtual_asset
     , underlying_asset
     , market
     , contract_address                              AS market_address
     , CAST(sizeChanged / 1e30 AS DOUBLE)              AS volume_usd
     , CAST(feeValue / 1e30 AS DOUBLE)                fee_usd
     , CAST(collateralValue / 1e30 AS DOUBLE)        AS margin_usd
     , (CASE
            WHEN isLong = false AND trade_type = 'Open' THEN 'Open Short'
            WHEN isLong = true AND trade_type = 'Open' THEN 'Open Long'
            WHEN isLong = false AND trade_type = 'Close' THEN 'Close Short'
            WHEN isLong = true AND trade_type = 'Close' THEN 'Close Long'
    END
    )                                                AS trade
     , 'xena'                                         AS project
     , 'v1'                                          AS version
     , 'xena'                                         AS frontend
     , account                                       AS trader
     , sizeChanged                                     AS volume_raw
     , evt_tx_hash                                       AS tx_hash
     , "from"                                        AS tx_from
     , to AS tx_to
     , evt_index AS evt_index
FROM complete_perp_tx
