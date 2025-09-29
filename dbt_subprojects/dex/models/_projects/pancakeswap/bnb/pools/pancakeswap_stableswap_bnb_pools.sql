{{ config(
    schema = 'pancakeswap_stableswap_bnb'
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
          blockchain = 'bnb'
        , project = 'pancakeswap'
        , version = 'stableswap'
        , pool_column_name = 'swapContract'
        , token0_column_name = 'tokenA'
        , token1_column_name = 'tokenB'
        , pool_created_event = source('pancakeswap_v2_bnb', 'PancakeStableSwapFactory_evt_NewStableSwapPair')
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
    min(evt_index) as evt_index, -- pick first
    token0,
    token1 
from 
pools 
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12 
-- there's some cases where a pool has multiple newstablepair event, using this to get the distinct values 