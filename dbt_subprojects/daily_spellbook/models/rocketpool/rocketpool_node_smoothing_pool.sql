{{ config(
    schema = 'rocketpool_ethereum',
    alias = 'node_smoothing_pool',
    materialized = 'table'
    )
}}

select
    node as node_address,
    max(evt_block_time) as evt_block_time,
    max_by(state, evt_block_time) as in_smoothing_pool
from
    {{ source('rocketpool_ethereum', 'rocketnodemanager_evt_nodesmoothingpoolstatechanged') }}
group by
    node
