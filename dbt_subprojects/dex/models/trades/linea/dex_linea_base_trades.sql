{{ config(
    schema = 'dex_linea'
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
    ref('sushiswap_v2_linea_base_trades')
    , ref('nile_linea_base_trades')
    , ref('echodex_linea_base_trades')
    , ref('secta_linea_base_trades')
    , ref('pancakeswap_v2_linea_base_trades')
    , ref('pancakeswap_v3_linea_base_trades')
    , ref('horizondex_linea_base_trades')
    , ref('uniswap_v3_linea_base_trades')
    , ref('lynex_fusion_linea_base_trades')
    , ref('swaap_v2_linea_base_trades')
    , ref('leetswap_linea_base_trades')
    , ref('etherex_v2_linea_base_trades')
    , ref('etherex_v3_linea_base_trades')
    , ref('oneinch_lop_linea_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'linea',
    base_models = base_models
) }}
