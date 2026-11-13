{{ config(
    schema = 'uniswap_v4_blast'
    , alias = 'aggregator_base_trades'
    , materialized = 'table'
    , file_format = 'delta'
    , tags=['static']
    )
}}

{{
    uniswap_compatible_v4_aggregator_trades(
        blockchain = 'blast'
        , swaps_model = ref('uniswap_v4_blast_swaps')
    )
}}
