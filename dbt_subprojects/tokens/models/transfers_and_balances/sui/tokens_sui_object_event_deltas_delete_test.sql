-- depends_on: {{ ref('tokens_sui_coin_object_anchor_state') }}

{{
  config(
    schema = 'tokens_sui',
    alias = 'object_event_deltas_delete_test',
    materialized = 'incremental',
    file_format = 'delta',
    partition_by = ['block_date'],
    incremental_strategy = 'delete+insert',
    unique_key = ['block_date', 'object_id', 'version'],
  )
}}

{{ tokens_sui_object_event_deltas_optimized_select('2025-06-01') }}
