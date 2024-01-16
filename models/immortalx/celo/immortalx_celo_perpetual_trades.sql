{{
  config(
    schema = 'immortalx_celo',
    alias = 'perpetual_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook = '{{ expose_spells(\'["celo"]\',
                                    "project",
                                    "immortalx",
                                    \'["tomfutago"]\') }}'
  )
}}

with

perp_events as (
  select
    p.evt_block_time as block_time,
    p.evt_block_number as block_number,
    concat('open-', if(isLong, 'long', 'short')) as trade,
    substring(m.symbol, 1, strpos(m.symbol, '/') - 1) as virtual_asset,
    'cUSD' as underlying_asset,
    m.symbol as market,
    p.size / 1e18 as volume_usd,
    p.tradeFee / 1e18 as fee_usd,
    p.margin / 1e18 as margin_usd,
    p.size as volume_raw,
    cast((p.size * 1e18 / p.margin) / 1e18 as int) as leverage,
    cast(null as double) as pnl,
    p.user as trader,
    p.contract_address as market_address,
    p.evt_tx_hash as tx_hash,
    p.evt_index
  from {{ source('immortalx_celo', 'Dex_evt_IncreasePosition') }} p
    join {{ source('immortalx_celo', 'Dex_evt_InitializeMarket') }} m on p.marketId = m.marketId
  {% if is_incremental() %}
  where {{ incremental_predicate('p.evt_block_time') }}
  {% endif %}

  union all
  
  select 
    p.evt_block_time as block_time,
    p.evt_block_number as block_number,
    concat(if(isLiquidated, 'liquidate', 'close'), '-', if(isLong, 'long', 'short')) as trade,
    substring(m.symbol, 1, strpos(m.symbol, '/') - 1) as virtual_asset,
    'cUSD' as underlying_asset,
    m.symbol as market,
    (p.decreaseSize + p.pnl) / 1e18 as volume_usd,
    p.tradeFee / 1e18 as fee_usd,
    p.decreaseMargin / 1e18 as margin_usd,
    p.decreaseSize + p.pnl as volume_raw,
    cast((p.decreaseSize * 1e18 / p.decreaseMargin) / 1e18 as int) as leverage,
    p.pnl / 1e18 as pnl,
    p.user as trader,
    p.contract_address as market_address,
    p.evt_tx_hash as tx_hash,
    p.evt_index
  from {{ source('immortalx_celo', 'Dex_evt_DecreasePosition') }} p
    join {{ source('immortalx_celo', 'Dex_evt_InitializeMarket') }} m on p.marketId = m.marketId
  {% if is_incremental() %}
  where {{ incremental_predicate('p.evt_block_time') }}
  {% endif %}
)

select
  'celo' as blockchain,
  'immortalx' as project,
  '1' as version,
  'immortalx' as frontend,
  cast(date_trunc('month', pe.block_time) as date) as block_month,
  cast(pe.block_time as date) as block_date,
  pe.block_time,
  pe.trade,
  pe.virtual_asset,
  pe.underlying_asset,
  pe.market,
  pe.market_address,
  pe.volume_usd,
  pe.fee_usd,
  pe.margin_usd,
  pe.trader,
  pe.volume_raw,
  pe.leverage,
  pe.pnl,
  tx."from" as tx_from,
  tx.to as tx_to,
  pe.tx_hash,
  pe.evt_index
from perp_events pe 
  join {{ source('celo', 'transactions') }} tx on pe.tx_hash = tx.hash and pe.block_time = tx.block_time
  {% if is_incremental() %}
    and {{ incremental_predicate('tx.block_time') }}
  {% endif %}
