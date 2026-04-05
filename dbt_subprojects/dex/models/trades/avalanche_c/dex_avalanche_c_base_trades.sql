{{ config(
    schema = 'dex_avalanche_c'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set base_models = [
    ref('uniswap_v4_avalanche_c_base_trades')
    , ref('uniswap_v3_avalanche_c_base_trades')
    , ref('uniswap_v2_avalanche_c_base_trades')
    , ref('airswap_avalanche_c_base_trades')
    , ref('sushiswap_v1_avalanche_c_base_trades')
    , ref('sushiswap_v2_avalanche_c_base_trades')
    , ref('fraxswap_avalanche_c_base_trades')
    , ref('trader_joe_v1_avalanche_c_base_trades')
    , ref('trader_joe_v2_avalanche_c_base_trades')
    , ref('trader_joe_v2_1_avalanche_c_base_trades')
    , ref('trader_joe_v2_2_avalanche_c_base_trades')
    , ref('balancer_v2_avalanche_c_base_trades')
    , ref('balancer_v3_avalanche_c_base_trades')
    , ref('glacier_v2_avalanche_c_base_trades')
    , ref('glacier_v3_avalanche_c_base_trades')
    , ref('gmx_avalanche_c_base_trades')
    , ref('pharaoh_avalanche_c_base_trades')
    , ref('kyberswap_avalanche_c_base_trades')
    , ref('platypus_finance_avalanche_c_base_trades')
    , ref('openocean_avalanche_c_base_trades')
    , ref('woofi_avalanche_c_base_trades')
    , ref('curvefi_avalanche_c_base_trades')
    , ref('hashflow_avalanche_c_base_trades')
    , ref('elk_finance_avalanche_c_base_trades')
    , ref('blackhole_v2_avalanche_c_base_trades')
    , ref('blackhole_v3_avalanche_c_base_trades')
    , ref('pharaoh_v3_legacy_avalanche_c_base_trades')
	, ref('pharaoh_v3_cl_avalanche_c_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'avalanche_c',
    base_models = base_models
) }}
