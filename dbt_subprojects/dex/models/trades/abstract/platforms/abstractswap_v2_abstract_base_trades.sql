{{ config(
    schema = 'abstractswap_v2_abstract'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , order_by = 'block_time'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}


with
    dexs as (
        {{
    uniswap_compatible_v2_trades(
        blockchain = 'abstract'
        , project = 'abstractswap'
        , version = '2'
        , Pair_evt_Swap = source('uniswap_abstract', 'pair_evt_swap')
        , Factory_evt_PairCreated = source('reservoir_swap_abstract', 'uniswapv2factory_evt_paircreated')
    )
    }}
    )

select *
from dexs
