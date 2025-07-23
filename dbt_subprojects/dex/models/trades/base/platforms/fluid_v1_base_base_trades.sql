{{
    config(
        schema = 'fluid_v1_base',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}
    
{% set native_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' %}
    
WITH 
  decoded_events AS (
      SELECT
          '1' AS version
          , t.evt_block_number AS block_number
          , t.evt_block_time AS block_time
          , t.to AS taker
          , CAST(NULL AS VARBINARY) AS maker
          , t.amountOut AS token_bought_amount_raw
          , t.amountIn AS token_sold_amount_raw
          , CASE 
              WHEN swap0to1 
              THEN bytearray_substring(p.dexDeploymentData_, 49, 20)
              ELSE bytearray_substring(p.dexDeploymentData_, 17, 20) 
          END AS token_bought_address
          , CASE 
              WHEN 
              NOT(swap0to1) 
              THEN bytearray_substring(p.dexDeploymentData_, 49, 20)
              ELSE bytearray_substring(p.dexDeploymentData_, 17, 20) 
          END AS token_sold_address
          , t.contract_address AS project_contract_address
          , t.evt_tx_hash AS tx_hash
          , t.evt_index
      FROM {{ source('fluid_base', 'FluidDexT1_evt_Swap') }} t
      INNER JOIN {{ source('fluid_base', 'fluiddexfactory_call_deploydex') }} p
          ON t.contract_address = p.output_dex_
      {% if is_incremental() %}
      WHERE {{ incremental_predicate('t.evt_block_time') }}
      {% endif %}
  )

SELECT
    'base' AS blockchain
    , 'fluid' AS project
    , dexs.version
    , CAST(date_trunc('month', dexs.block_time) AS date) AS block_month
    , CAST(date_trunc('day', dexs.block_time) AS date) AS block_date
    , dexs.block_time
    , dexs.block_number
    , dexs.token_bought_amount_raw
    , dexs.token_sold_amount_raw
    , IF(dexs.token_bought_address = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee, {{native_address}}, dexs.token_bought_address) AS token_bought_address
    , IF(dexs.token_sold_address = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee, {{native_address}}, dexs.token_sold_address) AS token_sold_address
    , dexs.taker
    , dexs.maker
    , dexs.project_contract_address
    , dexs.tx_hash
    , dexs.evt_index
FROM decoded_events dexs 
