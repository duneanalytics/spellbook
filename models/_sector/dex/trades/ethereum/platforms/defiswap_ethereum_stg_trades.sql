{{ config(
    schema = 'defiswap_ethereum',
    alias ='stg_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index']
    )
}}

{{
    dex_fork_v2_base_trades(
        blockchain = 'ethereum'
        , project = 'defiswap'
        , version = '1'
        , Pair_evt_Swap = source('defiswap_ethereum', 'CroDefiSwapPair_evt_Swap')
        , Factory_evt_PairCreated = source('crodefi_ethereum', 'CroDefiSwapFactory_evt_PairCreated')
    )
}}