{{ config(
    schema = 'dex_flow'
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
    ref('izumi_finance_v3_flow_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'flow',
    base_models = base_models
) }}
