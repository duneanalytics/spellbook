{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'capital_pool_totals',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date'],
    post_hook = '{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "nexusmutual",
                                \'["tomfutago"]\') }}'
  )
}}

with

transfer_combined as (
  select * from {{ ref('nexusmutual_ethereum_capital_pool_transfers') }}
),

lido_oracle as (
  select
    1 as anchor,
    evt_block_time as block_time,
    date_trunc('day', evt_block_time) as block_date,
    cast(postTotalPooledEther as double) / cast(totalShares as double) as rebase
  from {{ source('lido_ethereum', 'LegacyOracle_evt_PostTotalShares') }}
  where evt_block_time >= timestamp '2021-05-26'
),

steth_adjusted_date as (
  select
    date_add('day', case when t.block_time < lo.block_time then -1 else 0 end, t.block_date) as block_date,
    t.amount as steth_amount
  from lido_oracle lo
    inner join transfer_combined t on lo.block_date = t.block_date
  where t.symbol = 'stETH'
),

steth_net_staking as (
  select
    1 as anchor,
    lo.block_date,
    sd.steth_amount,
    lo.rebase as rebase2
  from lido_oracle lo
    inner join (
      select block_date, sum(steth_amount) as steth_amount
      from steth_adjusted_date
      group by 1
     ) sd on lo.block_date = sd.block_date
),

steth_expanded_rebase as (
  select
    lo.block_date,
    ns.steth_amount,
    lo.rebase,
    ns.rebase2
  from steth_net_staking ns
    inner join lido_oracle lo on ns.anchor = lo.anchor
  where ns.block_date <= lo.block_date
),

steth_running_total as (
  select distinct
    block_date,
    sum(steth_amount * rebase / rebase2) over (partition by block_date) as steth_total
  from steth_expanded_rebase
),

chainlink_oracle_nxmty_price as (
  select
    block_date,
    avg(oracle_price) as nxmty_price
  from {{ ref('chainlink_ethereum_price_feeds') }}
  where proxy_address = 0xcc72039a141c6e34a779ef93aef5eb4c82a893c7 -- Nexus wETH Reserves
    and block_time > timestamp '2022-08-15'
  group by 1
),

nxmty_running_total as (
  select
    cop.block_date,
    sum(t.amount) over (order by cop.block_date) as nxmty_total,
    sum(t.amount) over (order by cop.block_date) * cop.nxmty_price as nxmty_in_eth_total
  from chainlink_oracle_nxmty_price cop
    left join (
      select block_date, sum(amount) as amount
      from transfer_combined
      where symbol = 'NXMTY'
      group by 1
    ) t on cop.block_date = t.block_date
),

transfer_totals as (
  select
    block_date,
    sum(case when symbol = 'ETH' then amount end) as eth_total,
    sum(case when symbol = 'DAI' then amount end) as dai_total,
    sum(case when symbol = 'rETH' then amount end) as reth_total,
    sum(case when symbol = 'USDC' then amount end) as usdc_total
  from transfer_combined
  group by 1
),

daily_avg_eth_prices as (
  select
    date_trunc('day', minute) as block_date,
    avg(price) as price_usd
  from {{ source('prices', 'usd') }}
  where symbol = 'ETH'
    and coalesce(blockchain, 'ethereum') = 'ethereum'
    and minute >= timestamp '2019-05-01'
  group by 1
),

daily_avg_dai_prices as (
  select
    date_trunc('day', minute) as block_date,
    avg(price) as price_usd
  from {{ source('prices', 'usd') }}
  where symbol = 'DAI'
    and coalesce(blockchain, 'ethereum') = 'ethereum'
    and minute >= timestamp '2019-11-13'
  group by 1
),

daily_avg_reth_prices as (
  select
    date_trunc('day', minute) as block_date,
    avg(price) as price_usd
  from {{ source('prices', 'usd') }}
  where symbol = 'rETH'
    and coalesce(blockchain, 'ethereum') = 'ethereum'
    and minute >= timestamp '2021-09-30'
  group by 1
),

daily_avg_usdc_prices as (
  select
    date_trunc('day', minute) as block_date,
    avg(price) as price_usd
  from {{ source('prices', 'usd') }}
  where symbol = 'USDC'
    and coalesce(blockchain, 'ethereum') = 'ethereum'
    and minute >= timestamp '2019-05-01'
  group by 1
),

day_sequence as (
  select cast(d.seq_date as timestamp) as block_date
  from (select sequence(date '2019-05-01', current_date, interval '1' day) as days) as days_s
    cross join unnest(days) as d(seq_date)
),

daily_running_totals as (
  select
    ds.block_date,
    sum(tt.eth_total) over (order by ds.block_date) as eth_total,
    sum(tt.dai_total) over (order by ds.block_date) as dai_total,
    sum(tt.reth_total) over (order by ds.block_date) as reth_total,
    sum(tt.usdc_total) over (order by ds.block_date) as usdc_total,
    coalesce(steth_rt.steth_total, lag(steth_rt.steth_total) over (order by ds.block_date), 0) as steth_total,
    coalesce(nxmty_rt.nxmty_total, lag(nxmty_rt.nxmty_total) over (order by ds.block_date), 0) as nxmty_total,
    coalesce(nxmty_rt.nxmty_in_eth_total, lag(nxmty_rt.nxmty_in_eth_total) over (order by ds.block_date), 0) as nxmty_eth_total
  from day_sequence ds
    left join transfer_totals tt on ds.block_date = tt.block_date
    left join steth_running_total steth_rt on ds.block_date = steth_rt.block_date
    left join nxmty_running_total nxmty_rt on ds.block_date = nxmty_rt.block_date
),

daily_running_totals_enriched as (
  select
    drt.block_date,
    -- ETH
    drt.eth_total,
    drt.eth_total * p_avg_eth.price_usd as avg_eth_usd_total,
    -- DAI
    drt.dai_total,
    drt.dai_total * p_avg_dai.price_usd as avg_dai_usd_total,
    drt.dai_total * p_avg_dai.price_usd / p_avg_eth.price_usd as avg_dai_eth_total,
    -- NXMTY
    drt.nxmty_total,
    drt.nxmty_eth_total,
    drt.nxmty_eth_total * p_avg_eth.price_usd as avg_nxmty_usd_total,
    -- stETH
    drt.steth_total,
    drt.steth_total * p_avg_eth.price_usd as avg_steth_usd_total,
    -- rETH
    drt.reth_total,
    drt.reth_total * p_avg_reth.price_usd as avg_reth_usd_total,
    drt.reth_total * p_avg_reth.price_usd / p_avg_eth.price_usd as avg_reth_eth_total,
    -- USDC
    drt.usdc_total,
    drt.usdc_total * p_avg_usdc.price_usd as avg_usdc_usd_total,
    drt.usdc_total * p_avg_usdc.price_usd / p_avg_eth.price_usd as avg_usdc_eth_total
  from daily_running_totals drt
    inner join daily_avg_eth_prices p_avg_eth on drt.block_date = p_avg_eth.block_date
    left join daily_avg_dai_prices p_avg_dai on drt.block_date = p_avg_dai.block_date
    left join daily_avg_reth_prices p_avg_reth on drt.block_date = p_avg_reth.block_date
    left join daily_avg_usdc_prices p_avg_usdc on drt.block_date = p_avg_usdc.block_date
)

select
  block_date,
  -- Capital Pool
  eth_total + nxmty_eth_total + steth_total + avg_dai_eth_total + avg_reth_eth_total + avg_usdc_eth_total as avg_capital_pool_eth_total,
  avg_eth_usd_total + avg_nxmty_usd_total + avg_steth_usd_total + avg_dai_usd_total + avg_reth_usd_total + avg_usdc_usd_total as avg_capital_pool_usd_total,
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
  avg_usdc_eth_total
from daily_running_totals_enriched
{% if is_incremental() %}
where {{ incremental_predicate('block_date') }}
{% endif %}
