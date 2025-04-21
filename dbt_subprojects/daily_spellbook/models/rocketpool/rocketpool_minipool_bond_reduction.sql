{{ config(
    schema = 'rocketpool_ethereum',
    alias = 'minipool_bond_reduction',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['minipool','evt_block_time']
)
}}

select
    minipool,
    evt_block_time,
    newBondAmount / 1e18 as new_bond_amount,
    0.14 as new_node_fee
from 
    {{ source('rocketpool_ethereum','RocketMinipoolBondReducer_evt_BeginBondReduction') }}