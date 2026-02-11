{{
    config(
        schema = 'dyorswap_v2_xlayer',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        pre_hook = "{{ create_schema(this) }}"
    )
}}

{{
    uniswap_compatible_v2_trades(
        blockchain = 'xlayer',
        project = 'dyorswap',
        version = '2',
        Pair_evt_Swap = source('dyorswap_v2_xlayer', 'DYORPair_evt_Swap'),
        Factory_evt_PairCreated = source('dyorswap_v2_xlayer', 'DYORFactory_evt_PairCreated')
    )
}}
