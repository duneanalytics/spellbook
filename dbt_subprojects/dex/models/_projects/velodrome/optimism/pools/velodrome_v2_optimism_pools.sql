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
    distinct 
    * 
from 
pools 
-- there's one single case of a velodrome pool being deployed twice (two events), using distinct here to filter it out