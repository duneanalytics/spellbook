{{
    config(
        schema = 'curvefi_base',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH token_swaps AS (
  SELECT
    evt_block_number AS block_number,
    TRY_CAST(evt_block_time AS TIMESTAMP(3) WITH TIME ZONE) AS block_time,
    evt_tx_from AS maker,
    evt_tx_to AS taker,
    in_amount AS token_sold_amount_raw,
    out_amount AS token_bought_amount_raw,
    TRY_CAST(route[1] AS VARBINARY) AS token_sold_address,
    TRY_CAST(route[CARDINALITY(route)] AS VARBINARY) AS token_bought_address,
    contract_address AS project_contract_address,
    evt_tx_hash AS tx_hash,
    evt_index AS evt_index
  FROM delta_prod.curvefi_base.CurveRouter_evt_Exchange
)
SELECT
  'base' AS blockchain,
  'curve' AS project,
  '1' AS version,
  TRY_CAST(DATE_TRUNC('month', token_swaps.block_time) AS DATE) AS block_month,
  TRY_CAST(DATE_TRUNC('day', token_swaps.block_time) AS DATE) AS block_date,
  token_swaps.block_time,
  token_swaps.block_number,
  token_swaps.token_sold_amount_raw,
  token_swaps.token_bought_amount_raw,
  token_swaps.token_sold_address,
  token_swaps.token_bought_address,
  token_swaps.maker,
  token_swaps.taker,
  token_swaps.project_contract_address,
  token_swaps.tx_hash,
  token_swaps.evt_index
FROM token_swaps