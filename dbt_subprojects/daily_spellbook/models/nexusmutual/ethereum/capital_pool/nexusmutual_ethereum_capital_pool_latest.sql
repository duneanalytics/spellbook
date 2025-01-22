{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'capital_pool_latest',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["ethereum"]\',
                                  spell_type = "project",
                                  spell_name = "nexusmutual",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with

daily_running_totals as (
  select
    block_date,
    avg_eth_usd_price,
    avg_dai_usd_price,
    avg_usdc_usd_price,
    avg_cbbtc_usd_price,
    -- Capital Pool
    avg_capital_pool_eth_total,
    avg_capital_pool_usd_total,
    -- ETH
    eth_total,
    avg_eth_usd_total,
    -- DAI
    dai_total,
    avg_dai_usd_total,
    avg_dai_eth_total,
    -- NXMTY
    nxmty_total,
    nxmty_eth_total,
    avg_nxmty_usd_total,
    -- stETH
    steth_total,
    avg_steth_usd_total,
    -- rETH
    reth_total,
    avg_reth_usd_total,
    avg_reth_eth_total,
    -- USDC
    usdc_total,
    avg_usdc_usd_total,
    avg_usdc_eth_total,
    -- cbBTC
    cbbtc_total,
    avg_cbbtc_usd_total,
    avg_cbbtc_eth_total,
    -- Cover Re USDC investment
    cover_re_usdc_total,
    avg_cover_re_usdc_usd_total,
    avg_cover_re_usdc_eth_total,
    -- AAVE positions
    aave_collateral_weth_total,
    avg_aave_collateral_weth_usd_total,
    aave_debt_usdc_total,
    avg_aave_debt_usdc_usd_total,
    avg_aave_debt_usdc_eth_total,
    row_number() over (order by block_date desc) as rn
  from {{ ref('nexusmutual_ethereum_capital_pool_totals') }}
)

select
  block_date,
  avg_eth_usd_price,
  avg_dai_usd_price,
  avg_usdc_usd_price,
  avg_cbbtc_usd_price,
  -- Capital Pool
  avg_capital_pool_eth_total,
  avg_capital_pool_usd_total,
  -- ETH
  eth_total,
  avg_eth_usd_total,
  -- DAI
  dai_total,
  avg_dai_usd_total,
  avg_dai_eth_total,
  -- NXMTY
  nxmty_total,
  nxmty_eth_total,
  avg_nxmty_usd_total,
  -- stETH
  steth_total,
  avg_steth_usd_total,
  -- rETH
  reth_total,
  avg_reth_usd_total,
  avg_reth_eth_total,
  -- USDC
  usdc_total,
  avg_usdc_usd_total,
  avg_usdc_eth_total,
  -- cbBTC
  cbbtc_total,
  avg_cbbtc_usd_total,
  avg_cbbtc_eth_total,
  -- Cover Re USDC investment
  cover_re_usdc_total,
  avg_cover_re_usdc_usd_total,
  avg_cover_re_usdc_eth_total,
  -- AAVE positions
  aave_collateral_weth_total,
  avg_aave_collateral_weth_usd_total,
  aave_debt_usdc_total,
  avg_aave_debt_usdc_usd_total,
  avg_aave_debt_usdc_eth_total
from daily_running_totals
where rn = 1
