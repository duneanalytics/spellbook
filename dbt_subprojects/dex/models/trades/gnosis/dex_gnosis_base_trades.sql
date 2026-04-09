{{ config(
    schema = 'dex_gnosis'
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
    ref('sushiswap_v1_gnosis_base_trades')
    , ref('sushiswap_v2_gnosis_base_trades')
    , ref('balancer_v2_gnosis_base_trades')
    , ref('balancer_v3_gnosis_base_trades')    
    , ref('honeyswap_v2_gnosis_base_trades')
    , ref('elk_finance_gnosis_base_trades')
    , ref('levinswap_gnosis_base_trades')
    , ref('swapr_gnosis_base_trades')
    , ref('uniswap_v3_gnosis_base_trades')
    , ref('swapr_v3_gnosis_base_trades')
    , ref('oneinch_lop_gnosis_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'gnosis',
    base_models = base_models
) }}
