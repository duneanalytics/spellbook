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

SELECT uniswap_v2_base_trades.*
FROM all_decoded_trades AS uniswap_v2_base_trades
LEFT JOIN {{ ref('oneinch_lop_own_trades') }} AS oneinch_lop_own_trades
USING (tx_hash, evt_index)
WHERE factory_address NOT IN (
    -- token mismatched pool
    0x558e40f696c61c30f99f03e017840232f8c595e5
    )
    AND oneinch_lop_own_trades.tx_hash IS NULL