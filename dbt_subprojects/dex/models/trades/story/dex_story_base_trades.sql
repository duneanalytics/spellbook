{{ config(
    schema = 'dex_story'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'table'
    , tags = ['static']
    , file_format = 'delta'
    )
}}

{% set base_models = [
    ref('story_hunt_story_base_trades')
    , ref('piperx_v2_story_base_trades')
    , ref('piperx_v3_story_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'story',
    base_models = base_models
) }}
