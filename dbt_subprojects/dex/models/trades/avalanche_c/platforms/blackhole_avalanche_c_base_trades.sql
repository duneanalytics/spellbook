{{
    config(
        schema = 'blackhole_avalanche_c',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v2_trades(
        blockchain = 'avalanche_c',
        project = 'blackhole',
        version = '1',
        Pairgenerator_evt_Paircreated = source('blackhole_avalanche_c', 'pairgenerator_evt_paircreated'),
        Pair_evt_Swap = source('blackhole_avalanche_c', 'pair_evt_swap')
        Algebrafactory_evt_Custompool = source('blackhole_avalanche_c', 'algebrafactory_evt_custompool'),
        Algebrapool_evt_Swap = source('blackhole_avalanche_c', 'algebrapool_evt_swap')
    )
}}
