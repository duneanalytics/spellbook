{{ config(
    schema = 'dex_megaeth'
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
    ref('kumbaya_megaeth_base_trades'),
    ref('prismfi_megaeth_base_trades'),
    ref('world_megaeth_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'megaeth',
    base_models = base_models
) }}
