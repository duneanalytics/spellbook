{{ config(
    schema = 'dex_corn'
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
    ref('camelot_yak_corn_base_trades')
    , ref('camelot_v2_corn_base_trades')
    , ref('camelot_v3_corn_base_trades')
    , ref('oku_corn_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'corn',
    base_models = base_models
) }}
