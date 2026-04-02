{{ config(
    schema = 'dex_mantle'
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
    ref('merchant_moe_mantle_base_trades')
    , ref('merchant_moe_v22_mantle_base_trades')
    , ref('fusionx_mantle_base_trades')
    , ref('agni_mantle_base_trades')
    , ref('swaap_v2_mantle_base_trades')
    , ref('clipper_mantle_base_trades')
    , ref('uniswap_v3_mantle_base_trades')
    , ref('tropicalswap_mantle_base_trades')
    , ref('carbon_defi_mantle_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'mantle',
    base_models = base_models
) }}
