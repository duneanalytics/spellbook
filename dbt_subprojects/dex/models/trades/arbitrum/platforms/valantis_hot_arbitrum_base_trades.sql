{{ config(
    schema = 'valantis_hot_arbitrum'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    valantis_compatible_hot_trades(
        blockchain = 'arbitrum',
        project = 'valantis',
        version = 'hot',
        HOT_evt_Swap = source('valantis_arbitrum', 'HOT_evt_HotSwap'),
        Pair_evt_Swap = source('valantis_arbitrum', 'SovereignPool_evt_Swap'),
        Factory_evt_PoolCreated = source('valantis_arbitrum', 'ProtocolFactory_evt_SovereignPoolDeployed')
    )
}}
