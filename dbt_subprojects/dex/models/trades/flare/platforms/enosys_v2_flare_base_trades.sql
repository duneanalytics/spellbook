{{ config(
    schema = 'enosys_v2_flare'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH dexs AS
(
    {{
    uniswap_compatible_v2_trades(
        blockchain = 'flare'
        , project = 'enosys'
        , version = '2'
        , Pair_evt_Swap = source('enosys_flare', 'EnosysDexPair_evt_Swap')
        , Factory_evt_PairCreated = source('enosys_flare', 'EnosysDexFactory_evt_PairCreated')
    )
    }}
)

SELECT 
    *
FROM 
    dexs
