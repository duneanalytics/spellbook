{{ config(
    schema = 'rocketpool_ethereum',
    alias = 'minipool_created_destroyed',
    materialized = 'table'
    )
}}

with created as (
    select
        minipool,
        node as node_address,
        contract_address,
        evt_block_time as created_time
    from
        {{ source('rocketpool_ethereum','rocketminipoolmanager_evt_minipoolcreated') }}
)
,
destroyed as (
    select
        minipool,
        node as node_address,
        contract_address,
        evt_block_time as destroyed_time
    from
        {{ source('rocketpool_ethereum','rocketminipoolmanager_evt_minipooldestroyed') }}
)

select
    created.minipool,
    created.node_address,
    created.contract_address,
    created.created_time,
    destroyed.destroyed_time
from created
left join destroyed on created.minipool = destroyed.minipool
