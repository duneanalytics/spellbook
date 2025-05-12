{{ config(
    schema = 'rocketpool_ethereum',
    alias = 'node_rpl_staking',
    materialized = 'table'
    )
}}

with
rpl_staked as (
    select
        "from" as node_address,
        cast(amount / 1e18 as double) as amount,
        evt_block_time
    from
        {{ source('rocketpool_ethereum','RocketNodeStaking_evt_RPLStaked') }}
)
,
rpl_withdrawn as (
    select
        to as node_address,
        -1 * cast(amount / 1e18 as double) as amount,
        evt_block_time
    from
        {{ source('rocketpool_ethereum','RocketNodeStaking_evt_RPLWithdrawn') }}
)
,
rpl_slashed as (
    select
        node as node_address,
        evt_block_time,
        -1 * cast(amount / 1e18 as double) as amount
    from
        {{ source('rocketpool_ethereum','RocketNodeStaking_evt_RPLSlashed') }}
)

select
    node_address,
    evt_block_time,
    amount,
    'staked' as cat
from rpl_staked
union all
select
    node_address,
    evt_block_time,
    amount,
    'withdrawn' as cat
from rpl_withdrawn
union all
select
    node_address,
    evt_block_time,
    amount,
    'slashed' as cat
from rpl_slashed