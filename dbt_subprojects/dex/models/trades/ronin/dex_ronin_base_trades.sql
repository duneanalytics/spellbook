{{ config(
    schema = 'dex_ronin'
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
    ref('katana_v2_ronin_base_trades')
    , ref('katana_v3_ronin_base_trades')
    , ref('tamadotmeme_v1_ronin_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'ronin',
    base_models = base_models
) }}
