{{ config(

        schema = 'dex_mass_decoding_mode',
        alias = 'uniswap_v3_base_trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index', 'token_bought_address', 'token_sold_address'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
        )
}}

{{uniswap_v3_forks_trades(
    blockchain = 'mode'
    , version = 'null'
    , project = 'null'
    , Pair_evt_Swap = ref('uniswap_v3_pool_decoding_mode')
    , Factory_evt_PoolCreated = ref('uniswap_v3_factory_decoding_mode')
)}}

