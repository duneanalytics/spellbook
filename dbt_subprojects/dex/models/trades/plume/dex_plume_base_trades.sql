{{ config(
    schema = 'dex_plume'
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
    ref('rooster_protocol_plume_base_trades')
    , ref('rooster_protocol_v2_plume_base_trades')
    , ref('izumi_finance_v2_plume_base_trades')
    , ref('izumi_finance_plume_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'plume',
    base_models = base_models
) }}
