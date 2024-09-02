{{
  config(
    schema = 'polymarket_polygon',
    alias = 'market_orders_matched',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time','asset_id','evt_index','tx_hash'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook = '{{ expose_spells(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "polymarket",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with market_orders_matched as 
(
  SELECT evt_tx_hash, makerAssetId, takerAssetId from {{ source('polymarket_polygon', 'NegRiskCTFExchange_evt_OrdersMatched') }}
  UNION ALL 
  SELECT evt_tx_hash, makerAssetId, takerAssetId from {{ source('polymarket_polygon', 'CTFExchange_evt_OrdersMatched') }}
)

Select of.* from {{ ref('polymarket_polygon_market_activity') }} of
INNER JOIN market_orders_matched om
      ON om.evt_tx_hash = of.evt_tx_hash
      AND (of.makerAssetId = om.makerAssetId OR of.makerAssetId = om.takerAssetId)