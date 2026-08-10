{{ config(
    schema = 'dex_cronos'
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
    ref('vvs_finance_v2_cronos_base_trades'),
    ref('vvs_finance_v3_cronos_base_trades'),
    ref('cronaswap_cronos_base_trades'),
    ref('ferro_cronos_base_trades'),
] %}

{{ dex_base_trades_macro(
    blockchain = 'cronos',
    base_models = base_models
) }}
