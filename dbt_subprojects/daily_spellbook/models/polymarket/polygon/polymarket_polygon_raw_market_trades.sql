{{
  config(
    schema = 'polymarket_polygon',
    alias = 'raw_market_trades',
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

with fpmm_markets as (
  select
    fpmm.evt_block_time,
    fpmm.evt_block_number,
    fpmm.fixedProductMarketMaker as fpmm_contract_address,
    ctf.condition_id,
    ctf.token0 as asset_id,
    fpmm.fee,
    fpmm.contract_address,
    fpmm.evt_index,
    fpmm.evt_tx_hash
  from {{ source('polymarketfactory_polygon', 'FixedProductMarketMakerFactory_evt_FixedProductMarketMakerCreation') }} fpmm
    cross join unnest(fpmm.conditionIds) as c(condition_id)
    inner join {{ ref('polymarket_polygon_base_ctf_tokens') }} ctf on c.condition_id = ctf.condition_id
  where fpmm.collateralToken = 0x2791bca1f2de4661ed88a30c99a7a9449aa84174 -- USDC
)

select
  t.evt_block_time as block_time,
  t.evt_block_number as block_number,
  'FPMM trade' as action,
  m.condition_id,
  m.asset_id,
  --t.outcomeIndex,
  t.returnAmount / 1e6 as amount,
  t.outcomeTokensSold / 1e6 as shares,
  (t.outcomeTokensSold / 1e6) / (t.returnAmount / 1e6) as price,
  t.feeAmount / 1e6 as fee,
  t.seller as maker,
  t.contract_address as taker,
  t.returnAmount as maker_amount_raw,
  t.outcomeTokensSold as taker_amount_raw,
  t.contract_address,
  t.evt_index,
  t.evt_tx_hash as tx_hash
from {{ source('polymarket_polygon', 'FixedProductMarketMaker_evt_FPMMSell') }} t
  inner join fpmm_markets m on t.contract_address = m.fpmm_contract_address
{% if is_incremental() %}
where {{ incremental_predicate('t.evt_block_time') }}
{% endif %}

union all

select
  t.evt_block_time as block_time,
  t.evt_block_number as block_number,
  'FPMM trade' as action,
  m.condition_id,
  m.asset_id,
  --t.outcomeIndex,
  t.investmentAmount / 1e6 as amount,
  t.outcomeTokensBought / 1e6 as shares,
  (t.outcomeTokensBought / 1e6) / (t.investmentAmount / 1e6) as price,
  t.feeAmount / 1e6 as fee,
  t.contract_address as maker,
  t.buyer as taker,
  t.investmentAmount as maker_amount_raw,
  t.outcomeTokensBought as taker_amount_raw,
  t.contract_address,
  t.evt_index,
  t.evt_tx_hash as tx_hash
from {{ source('polymarket_polygon', 'FixedProductMarketMaker_evt_FPMMBuy') }} t
  inner join fpmm_markets m on t.contract_address = m.fpmm_contract_address
{% if is_incremental() %}
where {{ incremental_predicate('t.evt_block_time') }}
{% endif %}

union all

select
  t.evt_block_time as block_time,
  t.evt_block_number as block_number,
  'CLOB trade' as action,
  ctf.condition_id,
  coalesce(nullif(t.makerAssetId, 0), nullif(t.takerAssetID, 0)) as asset_id,
  if(t.makerAssetId = 0, t.makerAmountFilled, t.takerAmountFilled) / 1e6 as amount,
  if(t.makerAssetId = 0, t.takerAmountFilled, t.makerAmountFilled) / 1e6 as shares,
  if(
    t.makerAssetId = 0,
    (t.makerAmountFilled / 1e6) / (t.takerAmountFilled / 1e6),
    (t.takerAmountFilled / 1e6) / (t.makerAmountFilled / 1e6)
  ) as price,
  t.fee / 1e6 as fee,
  t.maker,
  t.taker,
  t.makerAmountFilled as maker_amount_raw,
  t.takerAmountFilled as taker_amount_raw,
  t.contract_address,
  t.evt_index,
  t.evt_tx_hash as tx_hash
from {{ source('polymarket_polygon', 'CTFExchange_evt_OrderFilled') }} t
  inner join {{ ref('polymarket_polygon_base_ctf_tokens') }} ctf on coalesce(nullif(t.makerAssetId, 0), nullif(t.takerAssetID, 0)) = ctf.token0
{% if is_incremental() %}
where {{ incremental_predicate('t.evt_block_time') }}
{% endif %}


union all

select
  t.evt_block_time as block_time,
  t.evt_block_number as block_number,
  'CLOB trade' as action,
  ctf.condition_id,
  coalesce(nullif(t.makerAssetId, 0), nullif(t.takerAssetID, 0)) as asset_id,
  if(t.makerAssetId = 0, t.makerAmountFilled, t.takerAmountFilled) / 1e6 as amount,
  if(t.makerAssetId = 0, t.takerAmountFilled, t.makerAmountFilled) / 1e6 as shares,
  if(
    t.makerAssetId = 0,
    (t.makerAmountFilled / 1e6) / (t.takerAmountFilled / 1e6),
    (t.takerAmountFilled / 1e6) / (t.makerAmountFilled / 1e6)
  ) as price,
  t.fee / 1e6 as fee,
  t.maker,
  t.taker,
  t.makerAmountFilled as maker_amount_raw,
  t.takerAmountFilled as taker_amount_raw,
  t.contract_address,
  t.evt_index,
  t.evt_tx_hash as tx_hash
from {{ source('polymarket_polygon', 'NegRiskCtfExchange_evt_OrderFilled') }} t
  inner join {{ ref('polymarket_polygon_base_ctf_tokens') }} ctf on coalesce(nullif(t.makerAssetId, 0), nullif(t.takerAssetID, 0)) = ctf.token0
{% if is_incremental() %}
where {{ incremental_predicate('t.evt_block_time') }}
{% endif %}
