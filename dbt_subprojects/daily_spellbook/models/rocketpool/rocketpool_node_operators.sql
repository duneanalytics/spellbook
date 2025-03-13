{{ config(
    schema = 'rocketpool_ethereum',
    alias = 'node_operators',
    materialized = 'table',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['node_address']
    )
}}

select
    node as node_address,
    evt_block_time
from
    {{ source('rocketpool_ethereum', 'rocketnodemanager_evt_noderegistered') }}