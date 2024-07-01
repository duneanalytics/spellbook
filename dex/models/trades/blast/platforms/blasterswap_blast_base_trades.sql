{{
    config(
        schema = 'blasterswap_blast',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH

dexs AS (
    {{
        uniswap_compatible_v2_trades(
            blockchain = 'blast',
            project = 'blasterswap',
            version = '1',
            Pair_evt_Swap = source('blasterswap_blast', 'BlasterswapV2Pair_ORBIT_USDB_evt_Swap'),
            Factory_evt_PairCreated = source('blasterswap_blast', 'BlasterswapV2Factory_evt_PairCreated')
        )
    }}

    UNION ALL

    {{
        uniswap_compatible_v2_trades(
            blockchain = 'blast',
            project = 'blasterswap',
            version = '1',
            Pair_evt_Swap = source('blasterswap_blast', 'BlasterswapV2Pair_PAC_USDB_evt_Swap'),
            Factory_evt_PairCreated = source('blasterswap_blast', 'BlasterswapV2Factory_evt_PairCreated')
        )
    }}

    UNION ALL

    {{
        uniswap_compatible_v2_trades(
            blockchain = 'blast',
            project = 'blasterswap',
            version = '1',
            Pair_evt_Swap = source('blasterswap_blast', 'BlasterswapV2Pair_PAC_WETH_evt_Swap'),
            Factory_evt_PairCreated = source('blasterswap_blast', 'BlasterswapV2Factory_evt_PairCreated')
        )
    }}

    UNION ALL

    {{
        uniswap_compatible_v2_trades(
            blockchain = 'blast',
            project = 'blasterswap',
            version = '1',
            Pair_evt_Swap = source('blasterswap_blast', 'BlasterswapV2Pair_USDB_WETH_evt_Swap'),
            Factory_evt_PairCreated = source('blasterswap_blast', 'BlasterswapV2Factory_evt_PairCreated')
        )
    }}
)

SELECT
    dexs.blockchain,
    dexs.project,
    dexs.version,
    dexs.block_month,
    dexs.block_date,
    dexs.block_time,
    dexs.block_number,
    dexs.token_bought_amount_raw,
    dexs.token_sold_amount_raw,
    dexs.token_bought_address,
    dexs.token_sold_address,
    dexs.taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    dexs.evt_index
FROM dexs
