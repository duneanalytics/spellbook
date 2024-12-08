{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'capital_pool_totals',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    post_hook = '{{ expose_spells(blockchains = \'["ethereum"]\',
                                  spell_type = "project",
                                  spell_name = "nexusmutual",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with

transfer_combined as (
  select * from {{ ref('nexusmutual_ethereum_capital_pool_transfers') }}
  where block_time >= timestamp '2019-05-23'
),

lido_oracle as (
  select
    1 as anchor,
    evt_block_time as block_time,
    date_trunc('day', evt_block_time) as block_date,
    cast(postTotalPooledEther as double) / cast(totalShares as double) as rebase
  from {{ source('lido_ethereum', 'LegacyOracle_evt_PostTotalShares') }}
  where evt_block_time >= timestamp '2021-05-26'
    and evt_block_time < timestamp '2023-05-16'
  union all
  select
    1 as anchor,
    evt_block_time as block_time,
    date_trunc('day', evt_block_time) as block_date,
    cast(postTotalEther as double) / cast(postTotalShares as double) as rebase
  from {{ source('lido_ethereum', 'steth_evt_TokenRebased') }}
  where evt_block_time >= timestamp '2023-05-16'
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
  from {{ source('chainlink_ethereum', 'price_feeds') }}
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
    sum(case when symbol = 'USDC' then amount end) as usdc_total,
    sum(case when symbol = 'cbBTC' then amount end) as cbbtc_total
  from transfer_combined
  group by 1
),

cover_re_usdc_investment as (
  select
    block_time,
    block_date,
    lead(block_date, 1, date_add('day', 1, current_date)) over (order by block_date) as next_block_date,
    amount,
    tx_hash
  from (
    select
      evt_block_time as block_time,
      date_trunc('day', evt_block_time) as block_date,
      investedUSDC / 1e6 as amount,
      evt_tx_hash as tx_hash,
      row_number() over (partition by date_trunc('day', evt_block_time) order by evt_block_time desc) as rn
    from {{ source('nexusmutual_ethereum', 'SafeTrackerNXMIS_evt_CoverReInvestmentUSDCUpdated') }}
  ) t
  where t.rn = 1
),

aave_current_market as (
  select block_time, block_date, symbol, reserve, liquidity_index, variable_borrow_index
  from (
    select
      r.evt_block_time as block_time,
      date_trunc('day', r.evt_block_time) as block_date,
      t.symbol,
      r.reserve,
      r.liquidityIndex as liquidity_index,
      r.variableBorrowIndex as variable_borrow_index,
      row_number() over (partition by date_trunc('day', r.evt_block_time), r.reserve order by r.evt_block_number desc, r.evt_index desc) as rn
    from {{ source('aave_v3_ethereum', 'Pool_evt_ReserveDataUpdated') }} r
      inner join {{ source('tokens', 'erc20') }} as t on r.reserve = t.contract_address and t.blockchain = 'ethereum'
    where r.evt_block_time >= timestamp '2024-05-23'
      and t.symbol in ('WETH', 'USDC')
  ) t
  where rn = 1
),

aave_supplied as (
  select
    date_trunc('day', s.block_time) as block_date,
    s.symbol,
    sum(s.amount / u.liquidityIndex * power(10, 27)) as atoken_amount
  from {{ ref('aave_ethereum_supply') }} s
    inner join {{ source('aave_v3_ethereum', 'Pool_evt_ReserveDataUpdated') }} u
       on u.evt_block_number = s.block_number
      and u.evt_index < s.evt_index
      and u.evt_tx_hash = s.tx_hash
      and u.reserve = s.token_address
  where s.block_time >= timestamp '2024-05-23'
    and coalesce(s.on_behalf_of, s.depositor) = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e --Advisory Board multisig
    and s.version = '3'
  group by 1, 2
),

aave_scaled_supplies as (
  select
    cm.block_date,
    cm.symbol,
    sum(s.atoken_amount) over (order by cm.block_date) * cm.liquidity_index / power(10, 27) as supplied_amount
  from aave_current_market cm
    left join aave_supplied s on cm.block_date = s.block_date and cm.symbol = s.symbol
  where cm.symbol = 'WETH'
),

aave_borrowed as (
  select
    date_trunc('day', b.block_time) as block_date,
    b.symbol,
    sum(b.amount / u.variableBorrowIndex * power(10, 27)) as atoken_amount
  from {{ ref('aave_ethereum_borrow') }} b
    inner join {{ source('aave_v3_ethereum', 'Pool_evt_ReserveDataUpdated') }} u
       on u.evt_block_number = b.block_number
      and u.evt_index < b.evt_index
      and u.evt_tx_hash = b.tx_hash
      and u.reserve = b.token_address
  where b.block_time >= timestamp '2024-05-23'
    and coalesce(b.on_behalf_of, b.borrower) = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e --Advisory Board multisig
    and b.version = '3'
  group by 1, 2
),

aave_scaled_borrows as (
  select
    cm.block_date,
    cm.symbol,
    sum(b.atoken_amount) over (order by cm.block_date) * cm.variable_borrow_index / power(10, 27) as borrowed_amount
  from aave_current_market cm
    left join aave_borrowed b on cm.block_date = b.block_date and cm.symbol = b.symbol
  where cm.symbol = 'USDC'
),

daily_avg_eth_prices as (
  select
    date_trunc('day', minute) as block_date,
    avg(price) as price_usd
  from {{ source('prices', 'usd') }}
  where symbol = 'ETH'
    and blockchain is null
    and contract_address is null
    and minute >= timestamp '2019-05-01'
  group by 1
),

daily_avg_dai_prices as (
  select
    date_trunc('day', minute) as block_date,
    avg(price) as price_usd
  from {{ source('prices', 'usd') }}
  where symbol = 'DAI'
    and blockchain = 'ethereum'
    and contract_address = 0x6b175474e89094c44da98b954eedeac495271d0f
    and minute >= timestamp '2019-07-12'
  group by 1
),

daily_avg_reth_prices as (
  select
    date_trunc('day', minute) as block_date,
    avg(price) as price_usd
  from {{ source('prices', 'usd') }}
  where symbol = 'rETH'
    and blockchain = 'ethereum'
    and contract_address = 0xae78736cd615f374d3085123a210448e74fc6393
    and minute >= timestamp '2021-09-30'
  group by 1
),

daily_avg_usdc_prices as (
  select
    date_trunc('day', minute) as block_date,
    avg(price) as price_usd
  from {{ source('prices', 'usd') }}
  where symbol = 'USDC'
    and blockchain = 'ethereum'
    and contract_address = 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
    and minute >= timestamp '2019-05-01'
  group by 1
),

daily_avg_cbbtc_prices as (
  select
    date_trunc('day', minute) as block_date,
    avg(price) as price_usd
  from {{ source('prices', 'usd') }}
  where symbol = 'cbBTC'
    and blockchain = 'ethereum'
    and contract_address = 0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf
    and minute >= timestamp '2024-10-21'
  group by 1
),

daily_reth_eth_prices as (
  select
    date_trunc('day', call_block_time) as block_date,
    avg(output_0 / 1e18) as avg_reth_eth_price
  from rocketpool_ethereum.RocketTokenRETH_call_getExchangeRate
  where call_block_time >= timestamp '2023-04-18'
  group by 1
),

day_sequence as (
  select cast(d.seq_date as timestamp) as block_date
  from (select sequence(date '2019-05-23', current_date, interval '1' day) as days) as days_s
    cross join unnest(days) as d(seq_date)
),

daily_running_totals as (
  select
    ds.block_date,
    sum(coalesce(tt.eth_total, 0)) over (order by ds.block_date) as eth_total,
    sum(coalesce(tt.dai_total, 0)) over (order by ds.block_date) as dai_total,
    sum(coalesce(tt.reth_total, 0)) over (order by ds.block_date) as reth_total,
    sum(coalesce(tt.usdc_total, 0)) over (order by ds.block_date) as usdc_total,
    sum(coalesce(tt.cbbtc_total, 0)) over (order by ds.block_date) as cbbtc_total,
    coalesce(
      steth_rt.steth_total,
      lag(steth_rt.steth_total, 1) over (order by ds.block_date),
      lag(steth_rt.steth_total, 2) over (order by ds.block_date),
      0
    ) as steth_total,
    coalesce(
      nxmty_rt.nxmty_total,
      lag(nxmty_rt.nxmty_total, 1) over (order by ds.block_date),
      lag(nxmty_rt.nxmty_total, 2) over (order by ds.block_date),
      0
    ) as nxmty_total,
    coalesce(
      nxmty_rt.nxmty_in_eth_total,
      lag(nxmty_rt.nxmty_in_eth_total, 1) over (order by ds.block_date),
      lag(nxmty_rt.nxmty_in_eth_total, 2) over (order by ds.block_date),
      0
    ) as nxmty_eth_total,
    coalesce(cre.amount, 0) as cover_re_usdc_total,
    coalesce(aave_s.supplied_amount, 0) as aave_collateral_weth_total,
    -1 * coalesce(aave_b.borrowed_amount, 0) as aave_debt_usdc_total
  from day_sequence ds
    left join transfer_totals tt on ds.block_date = tt.block_date
    left join steth_running_total steth_rt on ds.block_date = steth_rt.block_date
    left join nxmty_running_total nxmty_rt on ds.block_date = nxmty_rt.block_date
    left join aave_scaled_supplies aave_s on ds.block_date = aave_s.block_date
    left join aave_scaled_borrows aave_b on ds.block_date = aave_b.block_date
    left join cover_re_usdc_investment cre on ds.block_date >= cre.block_date and ds.block_date < cre.next_block_date
),

daily_running_totals_enriched as (
  select
    drt.block_date,
    coalesce(p_avg_eth.price_usd, 0) as avg_eth_usd_price,
    coalesce(p_avg_dai.price_usd, 0) as avg_dai_usd_price,
    coalesce(p_avg_usdc.price_usd, 0) as avg_usdc_usd_price,
    coalesce(p_avg_cbbtc.price_usd, 0) as avg_cbbtc_usd_price,
    -- ETH
    coalesce(drt.eth_total, 0) as eth_total,
    coalesce(drt.eth_total * p_avg_eth.price_usd, 0) as avg_eth_usd_total,
    -- DAI
    coalesce(drt.dai_total, 0) as dai_total,
    coalesce(drt.dai_total * p_avg_dai.price_usd, 0) as avg_dai_usd_total,
    coalesce(drt.dai_total * p_avg_dai.price_usd / p_avg_eth.price_usd, 0) as avg_dai_eth_total,
    -- NXMTY
    coalesce(drt.nxmty_total, 0) as nxmty_total,
    coalesce(drt.nxmty_eth_total, 0) as nxmty_eth_total,
    coalesce(drt.nxmty_eth_total * p_avg_eth.price_usd, 0) as avg_nxmty_usd_total,
    -- stETH
    coalesce(drt.steth_total, 0) as steth_total,
    coalesce(drt.steth_total * p_avg_eth.price_usd, 0) as avg_steth_usd_total,
    -- rETH
    coalesce(drt.reth_total, 0) as reth_total,
    coalesce(drt.reth_total * p_avg_reth.price_usd, 0) as avg_reth_usd_total,
    coalesce(drt.reth_total * p_reth_eth.avg_reth_eth_price, 0) as avg_reth_eth_total,
    -- USDC
    coalesce(drt.usdc_total, 0) as usdc_total,
    coalesce(drt.usdc_total * p_avg_usdc.price_usd, 0) as avg_usdc_usd_total,
    coalesce(drt.usdc_total * p_avg_usdc.price_usd / p_avg_eth.price_usd, 0) as avg_usdc_eth_total,
    -- cbBTC
    coalesce(drt.cbbtc_total, 0) as cbbtc_total,
    coalesce(drt.cbbtc_total * p_avg_cbbtc.price_usd, 0) as avg_cbbtc_usd_total,
    coalesce(drt.cbbtc_total * p_avg_cbbtc.price_usd / p_avg_eth.price_usd, 0) as avg_cbbtc_eth_total,
    -- Cover Re USDC investment
    coalesce(drt.cover_re_usdc_total, 0) as cover_re_usdc_total,
    coalesce(drt.cover_re_usdc_total * p_avg_usdc.price_usd, 0) as avg_cover_re_usdc_usd_total,
    coalesce(drt.cover_re_usdc_total * p_avg_usdc.price_usd / p_avg_eth.price_usd, 0) as avg_cover_re_usdc_eth_total,
    -- AAVE positions
    coalesce(drt.aave_collateral_weth_total, 0) as aave_collateral_weth_total,
    coalesce(drt.aave_collateral_weth_total * p_avg_eth.price_usd, 0) as avg_aave_collateral_weth_usd_total,
    coalesce(drt.aave_debt_usdc_total, 0) as aave_debt_usdc_total,
    coalesce(drt.aave_debt_usdc_total * p_avg_usdc.price_usd, 0) as avg_aave_debt_usdc_usd_total,
    coalesce(drt.aave_debt_usdc_total * p_avg_usdc.price_usd / p_avg_eth.price_usd, 0) as avg_aave_debt_usdc_eth_total
  from daily_running_totals drt
    inner join daily_avg_eth_prices p_avg_eth on drt.block_date = p_avg_eth.block_date
    left join daily_avg_dai_prices p_avg_dai on drt.block_date = p_avg_dai.block_date
    left join daily_avg_reth_prices p_avg_reth on drt.block_date = p_avg_reth.block_date
    left join daily_avg_usdc_prices p_avg_usdc on drt.block_date = p_avg_usdc.block_date
    left join daily_avg_cbbtc_prices p_avg_cbbtc on drt.block_date = p_avg_cbbtc.block_date
    left join daily_reth_eth_prices p_reth_eth on drt.block_date = p_reth_eth.block_date
)

select
  block_date,
  avg_eth_usd_price,
  avg_dai_usd_price,
  avg_usdc_usd_price,
  avg_cbbtc_usd_price,
  -- Capital Pool totals
  eth_total + nxmty_eth_total + steth_total + avg_dai_eth_total + avg_reth_eth_total + avg_usdc_eth_total + avg_cbbtc_eth_total
    + avg_cover_re_usdc_eth_total + aave_collateral_weth_total + avg_aave_debt_usdc_eth_total as avg_capital_pool_eth_total,
  avg_eth_usd_total + avg_nxmty_usd_total + avg_steth_usd_total + avg_dai_usd_total + avg_reth_usd_total + avg_usdc_usd_total + avg_cbbtc_usd_total
    + avg_cover_re_usdc_usd_total + avg_aave_collateral_weth_usd_total + avg_aave_debt_usdc_usd_total as avg_capital_pool_usd_total,
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
from daily_running_totals_enriched
{% if is_incremental() %}
where {{ incremental_predicate('block_date') }}
{% endif %}
