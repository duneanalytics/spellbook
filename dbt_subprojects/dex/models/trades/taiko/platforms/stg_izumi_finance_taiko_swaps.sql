{{
    config(
        schema = 'stg_izumi_finance_taiko',
        alias = 'swaps',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH base_swaps AS (
  SELECT
      contract_address
    , evt_tx_hash
    , evt_tx_from
    , evt_tx_to
    , evt_tx_index
    , evt_index
    , evt_block_number
    , amountX
    , amountY
    , currentPoint
    , fee
    , sellXEarnY
    , tokenX
    , tokenY
    -- Standardized/derived columns for downstream compatibility
    , evt_tx_hash AS tx_hash
    , evt_block_number AS block_number
    , evt_block_date AS block_date
    , evt_block_time AS block_time
    , amountX AS amount0
    , amountY AS amount1
    , tokenX AS token0
    , tokenY AS token1
    , CASE WHEN sellXEarnY THEN tokenY ELSE tokenX END AS token_bought_address
    , CASE WHEN sellXEarnY THEN tokenX ELSE tokenY END AS token_sold_address
    , CASE WHEN sellXEarnY THEN amountY ELSE amountX END AS token_bought_amount_raw
    , CASE WHEN sellXEarnY THEN amountX ELSE amountY END AS token_sold_amount_raw
    , 'taiko' AS blockchain
    , 'izumi_finance' AS project
    , 'v3' AS version
  FROM {{ source('izumi_finance_taiko', 'iziswappool_evt_swap') }}
)
SELECT * FROM base_swaps