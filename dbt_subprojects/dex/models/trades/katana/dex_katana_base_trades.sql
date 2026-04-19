{{ config(
    schema = 'dex_katana'
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
    ref('sushiswap_v1_katana_base_trades')
    , ref('sushiswap_v2_katana_base_trades')
    , ref('blade_katana_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'katana',
    base_models = base_models
) }}
