{{ config(
    schema = 'dex_sonic'
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
    ref('beets_v2_sonic_base_trades')
    , ref('beets_v3_sonic_base_trades')
    , ref('wagmi_sonic_base_trades')
    , ref('equalizer_sonic_base_trades')
    , ref('shadow_sonic_base_trades')
    , ref('silverswap_sonic_base_trades')
    , ref('uniswap_v3_sonic_base_trades')
    , ref('tapio_sonic_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'sonic',
    base_models = base_models
) }}
