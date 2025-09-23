{{ config(
    schema = 'velodrome_v2_optimism'
    , alias = 'pools'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['id']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.creation_block_time')]
    )
}}

with 

pools as (
{{
    uniswap_compatible_pools(
          blockchain = 'optimism'
        , project = 'veloodrome'
        , version = '2'
        , pool_column_name = 'pool'
        , token0_column_name = 'token0'
        , token1_column_name = 'token1'
        , pool_created_event = source('velodrome_v2_optimism', 'PoolFactory_evt_PoolCreated')
    )
}}

) 

select 
    blockchain,
    project,
    version,
    contract_address,
    creation_block_time,
    creation_block_number,
    id,
    fee,
    tx_hash,
    min(evt_index) as evt_index -- pick first
    token0,
    token1 
from 
pools 
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12 
-- there's one single case of a velodrome pool being deployed twice (two events), using distinct here to filter it out