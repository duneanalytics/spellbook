{{ config(
    schema = 'dex_taiko'
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
    ref('oku_v3_taiko_base_trades')
    , ref('unagi_v3_taiko_base_trades')
    , ref('izumi_finance_taiko_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'taiko',
    base_models = base_models
) }}
