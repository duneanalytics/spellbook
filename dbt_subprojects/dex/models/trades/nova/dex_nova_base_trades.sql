{{ config(
    schema = 'dex_nova'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'table'
    , tags = ['static']
    , file_format = 'delta'
    )
}}

{% set base_models = [
    ref('sushiswap_nova_base_trades')
    , ref('rcpswap_nova_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'nova',
    base_models = base_models
) }}
