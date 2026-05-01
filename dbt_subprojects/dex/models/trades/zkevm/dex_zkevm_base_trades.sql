{{ config(
    schema = 'dex_zkevm'
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
    ref('balancer_v2_zkevm_base_trades')
    , ref('pancakeswap_v2_zkevm_base_trades')
    , ref('pancakeswap_v3_zkevm_base_trades')
    , ref('clipper_zkevm_base_trades')
    , ref('sushiswap_v2_zkevm_base_trades')
    , ref('leetswap_zkevm_base_trades')
    , ref('quickswap_v3_zkevm_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'zkevm',
    base_models = base_models
) }}
