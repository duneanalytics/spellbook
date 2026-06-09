{{ config(
    schema = 'uniswap_v4_monad'
    , alias = 'aggregator_base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v4_aggregator_trades(
        blockchain = 'monad'
        , swaps_model = ref('uniswap_v4_monad_swaps')
    )
}}
