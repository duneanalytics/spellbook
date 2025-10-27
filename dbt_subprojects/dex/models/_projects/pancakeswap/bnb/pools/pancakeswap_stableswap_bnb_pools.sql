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
    min_by(contract_address, creation_block_number) as contract_address,
    min_by(creation_block_time, creation_block_number) as creation_block_time,
    min(creation_block_number) as creation_block_number,
    id,
    fee,
    min_by(tx_hash, creation_block_number) as tx_hash,
    min_by(evt_index, creation_block_number) as evt_index, -- pick first
    token0,
    token1 
from 
pools 
group by 1, 2, 3, 7, 8, 11, 12 
-- there's some cases where a pool has multiple newstablepair event, using this to get the distinct values 