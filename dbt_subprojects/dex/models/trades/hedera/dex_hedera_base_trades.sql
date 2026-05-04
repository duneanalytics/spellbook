{{ config(
    schema = 'dex_hedera',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
) }}

{% set base_models = [
    ref('saucerswap_v1_hedera_base_trades'),
    ref('saucerswap_v2_hedera_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'hedera',
    base_models = base_models
) }}
