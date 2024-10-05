{{
  config(
    schema = 'polymarket_polygon',
    alias = 'market_trades',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "polymarket",
                                  contributors = \'["tomfutago, 0xboxer"]\') }}'
  )
}}

select
  t.block_number,
  t.block_time,
  t.tx_hash,
  t.evt_index,
  t.action,
  t.contract_address,
  t.condition_id,
  md.event_market_name,
  md.question,
  md.polymarket_link,
  md.token_outcome,
  md.neg_risk,
  t.asset_id,
  t.price,
  t.amount,
  t.shares,
  t.fee,
  t.maker,
  t.taker,
  md.unique_key,
  md.token_outcome_name
from {{ ref('polymarket_polygon_raw_market_trades') }} t
left join {{ ref('polymarket_polygon_market_details') }} md on t.condition_id = md.condition_id
