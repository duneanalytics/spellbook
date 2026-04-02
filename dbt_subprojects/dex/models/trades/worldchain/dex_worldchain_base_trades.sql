{{ config(
    schema = 'dex_worldchain'
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
    ref('uniswap_v3_worldchain_base_trades')
    , ref('uniswap_v4_worldchain_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'worldchain',
    base_models = base_models
) }}
