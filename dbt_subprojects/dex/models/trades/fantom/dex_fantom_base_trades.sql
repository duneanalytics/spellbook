{{ config(
    schema = 'dex_fantom'
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
    ref('sushiswap_v1_fantom_base_trades')
    , ref('sushiswap_v2_fantom_base_trades')
    , ref('spiritswap_fantom_base_trades')
    , ref('spookyswap_fantom_base_trades')
    , ref('wigoswap_fantom_base_trades')
    , ref('equalizer_fantom_base_trades')
    , ref('spartacus_exchange_fantom_base_trades')
    , ref('openocean_fantom_base_trades')
    , ref('beethoven_x_fantom_base_trades')
    , ref('curvefi_fantom_base_trades')
    , ref('solidly_v3_fantom_base_trades')
    , ref('yoshiexchange_fantom_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'fantom',
    base_models = base_models
) }}
