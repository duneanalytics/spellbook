{{ config(
    schema = 'dex_hemi'
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
    ref('oku_v3_hemi_base_trades')
    , ref('izumi_finance_hemi_base_trades')
    , ref('sushiswap_v2_hemi_base_trades')
    , ref('sushiswap_v3_hemi_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'hemi',
    base_models = base_models
) }}
