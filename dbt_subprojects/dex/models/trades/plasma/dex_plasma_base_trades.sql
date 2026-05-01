{{ config(
    schema = 'dex_plasma'
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
    ref('uniswap_v3_plasma_base_trades')
    , ref('fluid_v1_plasma_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'plasma',
    base_models = base_models
) }}
