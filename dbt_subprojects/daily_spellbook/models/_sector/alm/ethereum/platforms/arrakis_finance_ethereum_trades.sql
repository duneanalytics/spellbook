{{ config(
    schema = 'arrakis_finance_ethereum'
    , alias = 'trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index', 'vault_address']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    arrakis_compatible_v2_trades(
        blockchain = 'ethereum'
        , project = 'arrakis'
        , version = '2'
        , dex = 'uniswap'
        , dex_version = '3'
        , Pair_evt_Mint = source('uniswap_v3_ethereum', 'Pair_evt_Mint')
        , Pair_evt_Burn = source('uniswap_v3_ethereum', 'Pair_evt_Burn')
        , Pair_evt_Swap = source('uniswap_v3_ethereum', 'Pair_evt_Swap')
        , Factory_evt_PoolCreated = source('uniswap_v3_ethereum', 'Factory_evt_PoolCreated')
        , ArrakisV2Factory_evt_VaultCreated = source('arrakis_finance_ethereum', 'ArrakisV2Factory_evt_VaultCreated')
    )
}}