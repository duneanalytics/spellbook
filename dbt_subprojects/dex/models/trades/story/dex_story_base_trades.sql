{{ config(
    schema = 'dex_story'
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
    ref('story_hunt_story_base_trades')
    , ref('piperx_v2_story_base_trades')
    , ref('piperx_v3_story_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'story',
    base_models = base_models
) }}
