{{ config(
    schema = 'dex_tempo'
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
    ref('tempo_exchange_tempo_base_trades')
    , ref('uniswap_v2_tempo_base_trades')
    , ref('uniswap_v3_tempo_base_trades')
    , ref('uniswap_v4_tempo_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'tempo',
    base_models = base_models
) }}
