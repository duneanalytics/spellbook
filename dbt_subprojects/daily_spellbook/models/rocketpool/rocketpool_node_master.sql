{{ config(
    schema = 'rocketpool_ethereum',
    alias = 'node_master',
    materialized = 'table',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "rocketpool",
                                \'["mtitus6"]\') }}'
    )
}}

with minipools as (
    select
        node_address,
        sum(case when beacon_amount_deposited > 0 then 1 else 0 end) as total_minipools,
        sum(case when beacon_amount_deposited > 1 and exited = false then 1 else 0 end) as active_minipools,
        sum(case when beacon_amount_deposited > 1 and exited = true then 1 else 0 end) as exited_minipools,
        sum(case when exited = false and beacon_amount_deposited > 1 then beacon_amount_deposited else 0 end)
            as active_effective_stake,
        sum(case when exited = false and beacon_amount_deposited > 1 then bond_amount else 0 end) as active_bond_amount
    from {{ ref('rocketpool_minipool_master') }}
    group by 1
)
,
rpl_stake as (
    select
        node_address,
        round(sum(amount), 8) as rpl_staked_amount
    from {{ ref('rocketpool_node_rpl_staking') }}
    group by 1
)
,
rpl_price as (
    select price as rpl_price_usd
    from {{ source('prices','usd_latest') }}
    where contract_address = 0xD33526068D116cE69F19A9ee46F0bd304F21A51f
)
,
weth_price as (
    select price as weth_price_usd
    from {{ source('prices','usd_latest') }}
    where contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
)

select
    nodes.node_address,
    nodes.evt_block_time as node_registered_time,
    coalesce(minipools.total_minipools, 0) as total_minipools,
    coalesce(minipools.active_minipools, 0) as active_minipools,
    coalesce(minipools.exited_minipools, 0) as exited_minipools,
    coalesce(minipools.active_effective_stake, 0) as active_effective_stake,
    coalesce(minipools.active_bond_amount, 0) as active_bond_amount,
    coalesce(minipools.active_effective_stake - minipools.active_bond_amount, 0) as active_borrowed_amount,
    coalesce(rpl_stake.rpl_staked_amount, 0) as rpl_staked_amount,
    (select rpl_price_usd from rpl_price)
    / (select weth_price_usd from weth_price) * rpl_stake.rpl_staked_amount as rpl_staked_amount_weth,
    case when minipools.active_effective_stake > 0
            then
                (
                    (select rpl_price_usd from rpl_price
                    ) / (select weth_price_usd from weth_price
                    ) * rpl_stake.rpl_staked_amount
                )
                / (minipools.active_effective_stake - minipools.active_bond_amount)
        else 0
    end as rpl_vs_borrowed_ratio,
    coalesce(smooth.in_smoothing_pool, false) as in_smoothing_pool,
    coalesce(smooth.evt_block_time, nodes.evt_block_time) as in_smoothing_pool_time
from {{ ref('rocketpool_node_operators') }} as nodes 
left join minipools on nodes.node_address = minipools.node_address
left join rpl_stake on nodes.node_address = rpl_stake.node_address
left join {{ ref('rocketpool_node_smoothing_pool') }} as smooth on nodes.node_address = smooth.node_address