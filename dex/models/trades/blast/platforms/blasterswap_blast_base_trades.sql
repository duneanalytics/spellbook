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

{% set evt_sources = [
    source('blasterswap_blast', 'BlasterswapV2Pair_ORBIT_USDB_evt_Swap')
    , source('blasterswap_blast', 'BlasterswapV2Pair_PAC_USDB_evt_Swap')
    , source('blasterswap_blast', 'BlasterswapV2Pair_PAC_USDB_evt_Swap')
    , source('blasterswap_blast', 'BlasterswapV2Pair_USDB_WETH_evt_Swap')
] %}

WITH

unioned_evt_sources AS (
    {% for evt_source in evt_sources %}
        SELECT
            *
        FROM
            {{ evt_source }}
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('evt_block_time') }}
        {% endif %}
        {% if not loop.last %}
        UNION
        {% endif %}
    {% endfor %}
)

, dexs AS (
    {{
        uniswap_compatible_v2_trades(
            blockchain = 'blast',
            project = 'blasterswap',
            version = '2',
            Pair_evt_Swap = 'unioned_evt_sources',
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
