{{ config(
    schema = 'dex_hyperevm'
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
    ref('prjx_v3_hyperevm_base_trades')
    , ref('hyperswap_v2_hyperevm_base_trades')
    , ref('hyperswap_v3_hyperevm_base_trades')
    , ref('hybra_v3_hyperevm_base_trades')
    , ref('balancer_v3_hyperevm_base_trades')
] %}
{{ dex_base_trades_macro(
    blockchain = 'hyperevm',
    base_models = base_models
) }}
