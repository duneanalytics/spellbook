{{ config(
    schema = 'dex_flare'
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
    ref('enosys_v2_flare_base_trades')
    , ref('sparkdex_v2_flare_base_trades')
    , ref('sparkdex_v3_flare_base_trades')
    , ref('blazeswap_flare_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'flare',
    base_models = base_models
) }}
