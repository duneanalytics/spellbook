{{ config(
    schema = 'uniswap_v4_polygon'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{#- native_token_address: the v4 PoolKey uses address(0) for native POL, but Dune's canonical
    polygon native address is the POL genesis contract 0x...1010 (dune.blockchains, tokens.erc20,
    prices, tokens.transfers all key on it; none map address(0)) -#}
{{
    uniswap_compatible_v4_trades(
        blockchain = 'polygon'
        , project = 'uniswap'
        , version = '4'
        , PoolManager_call_Swap = source('uniswap_v4_polygon', 'PoolManager_call_Swap')
        , PoolManager_evt_Swap = source('uniswap_v4_polygon', 'PoolManager_evt_Swap')
        , pool_manager_addr = '0x67366782805870060151383f4bbff9dab53e5cd6'
        , start_date = '2025-01-22'
        , native_token_address = '0x0000000000000000000000000000000000001010'
    )
}}