{{ config(
    schema = 'dex_robinhood'
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
    ref('uniswap_v2_robinhood_base_trades')
    , ref('uniswap_v3_robinhood_base_trades')
    , ref('uniswap_v4_robinhood_base_trades')
    , ref('swaphood_v2_robinhood_base_trades')
    , ref('swaphood_v3_robinhood_base_trades')
    , ref('pancakeswap_v2_robinhood_base_trades')
    , ref('pancakeswap_v3_robinhood_base_trades')
    , ref('sheriff_v2_robinhood_base_trades')
    , ref('sheriff_v4_robinhood_base_trades')
    , ref('sushiswap_v2_robinhood_base_trades')
    , ref('sushiswap_v3_robinhood_base_trades')
    , ref('robinswap_v3_robinhood_base_trades')
    , ref('aeon_protocol_v1_robinhood_base_trades')
    , ref('catnip_v1_robinhood_base_trades')
    , ref('up_v3_robinhood_base_trades')
    , ref('frothswap_v1_robinhood_base_trades')
    , ref('thor_v3_robinhood_base_trades')
    , ref('factory_b4ec911f_v1_robinhood_base_trades')
    , ref('factory_c4c1d1d9_v1_robinhood_base_trades')
    , ref('factory_46c695d1_v3_robinhood_base_trades')
    , ref('apex_cl_v3_robinhood_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'robinhood',
    base_models = base_models
) }}
