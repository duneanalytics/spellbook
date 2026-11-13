{{ config(
    schema = 'dex_superseed'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'table'
    , tags = ['static']
    , file_format = 'delta'
    )
}}

{% set base_models = [
     ref('velodrome_superseed_base_trades')
     , ref('bulletx_v2_superseed_base_trades')
     , ref('bulletx_v3_superseed_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'superseed',
    base_models = base_models
) }}
