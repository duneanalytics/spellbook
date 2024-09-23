{{ config(

        schema = 'dex_mass_decoding_ethereum',
        alias = 'uniswap_v2_base_trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index', 'token_bought_address', 'token_sold_address'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
        )
}}

WITH all_decoded_trades AS (
    {{
            uniswap_v2_forks_trades(
                blockchain = 'ethereum'
                , version = 'null'
                , project = 'null'
                , Pair_evt_Swap = ref('uniswap_v2_pool_decoding_ethereum')
                , Factory_evt_PairCreated = ref('uniswap_v2_factory_decoding_ethereum')
            )
    }}
)

SELECT *
FROM all_decoded_trades
WHERE factory_address NOT IN (
    -- 1inch LOP trades related
    0x9a27cb5ae0b2cee0bb71f9a85c0d60f3920757b4
    , 0x43ec799eadd63848443e2347c49f5f52e8fe0f6f
    , 0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f
    , 0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac
)