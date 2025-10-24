{{ config(
    schema = 'eulerswap_ethereum'
    , alias = 'trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    eulerswap_downstream_trades(
        blockchain = 'ethereum'
        , project = 'eulerswap'
        , version = '1'
        , eulerswapinstance_evt_swap = source('eulerswap_ethereum', 'eulerswapinstance_evt_swap')
        , eulerswap_pools_created = ref('eulerswap_ethereum_pool_creations') 
    )
}}