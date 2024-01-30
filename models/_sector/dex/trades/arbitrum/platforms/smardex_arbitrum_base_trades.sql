{{
    config(
        schema = 'smardex_arbitrum',
        alias ='base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v3_trades(
        blockchain = 'arbitrum',
        project = 'smardex',
        version = '1',
        Pair_evt_Swap = source('smardex_arbitrum', 'SmardexPair_evt_Swap'),
        Factory_evt_PoolCreated = source('smardex_arbitrum', 'SmardexFactory_evt_PairCreated'),
        taker_column_name = 'to',
        maker_column_name = 'contract_address',
        optional_columns = null,
        pair_column_name = 'pair'
    )
}}