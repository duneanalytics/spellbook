{{ config(
    schema = 'dex_sophon'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'table'
    , tags = ['static']
    , file_format = 'delta'
    )
}}

{% set base_models = [
    ref('syncswap_v2_sophon_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'sophon',
    base_models = base_models
) }}
