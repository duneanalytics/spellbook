{{ config(
    schema = 'uniswap_v2_ethereum'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set weth_ubomb_wash_trading_pair = '0xed9c854cb02de75ce4c9bba992828d6cb7fd5c71' %}
{% set weth_weth_wash_trading_pair = '0xf9c1fa7d41bf44ade1dd08d37cc68f67ae75bf92' %}
{% set feg_eth_wash_trading_pair = '0x854373387e41371ac6e307a1f29603c6fa10d872' %}

WITH dexs AS
(
    {{
    uniswap_compatible_v2_trades(
        blockchain = 'ethereum'
        , project = 'uniswap'
        , version = '2'
        , Pair_evt_Swap = source('uniswap_v2_ethereum', 'Pair_evt_Swap')
        , Factory_evt_PairCreated = source('uniswap_v2_ethereum', 'Factory_evt_PairCreated')
    )
    }}
)

SELECT 
    *
FROM 
    dexs
WHERE 
    project_contract_address NOT IN (
        {{weth_ubomb_wash_trading_pair}}
        , {{weth_weth_wash_trading_pair}}
        , {{feg_eth_wash_trading_pair}}
        )