{{
    config(
        schema = 'stg_izumi_finance_taiko',
        alias = 'pool',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH base_pool AS (
  SELECT
      chain
    , contract_address
    , evt_tx_hash as tx_hash
    , evt_tx_from
    , evt_tx_to
    , evt_tx_index
    , evt_index
    , evt_block_time
    , evt_block_number
    , evt_block_time AS block_time
    , fee
    , pointDelta
    , pool
    , tokenX
    , tokenY
    , tokenX AS token0
    , tokenY AS token1
  FROM {{ source('izumi_finance_multichain', 'iziswapfactory_evt_newpool') }}
)
SELECT * FROM base_pool