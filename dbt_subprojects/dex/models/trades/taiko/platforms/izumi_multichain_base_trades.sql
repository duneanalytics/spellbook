{{
    config(
        schema = 'izumi_multichain',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

with izumi_swaps as (
    select
        *,
        amountX as amount0,
        amountY as amount1,
        tokenX as token0,
        tokenY as token1
    from {{ source('izumi_multichain', 'iziswappool_evt_swap') }}
)

{{
    uniswap_compatible_v3_trades(
        blockchain = 'taiko',
        project = 'izumi',
        version = '1',
        Pair_evt_Swap = 'izumi_swaps',
        Factory_evt_PoolCreated = source('izumi_multichain', 'iziswapfactory_evt_newpool'),
        taker_column_name = 'evt_tx_to'
    )
}}
