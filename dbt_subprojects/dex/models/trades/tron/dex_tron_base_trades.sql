{{
  config(
    schema = 'dex_tron',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

{% set base_models = [
  ref('sunswap_v1_tron_base_trades'),
  ref('sunswap_v2_tron_base_trades'),
  ref('sunswap_v3_tron_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'tron',
    base_models = base_models
) }}
