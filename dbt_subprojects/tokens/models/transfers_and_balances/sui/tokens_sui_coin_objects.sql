{{
  config(
    schema = 'tokens_sui',
    alias = 'coin_objects',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    merge_skip_unchanged = true,
    tags = ['sui', 'tokens', 'transfers'],
  )
}}

-- Pre-filtered subset of sui.objects containing only Coin<T> mutations.
-- Reduces downstream scans from 12.7 TB (full objects) to ~2 TB (coin-only).
-- Stores only the columns needed for transfer computation.
--
-- This model is the single scan point for the raw objects table. Downstream
-- models (base_transfers) read from this instead of sui.objects directly,
-- avoiding repeated full-table scans with the same filters.

{% set sui_transfer_start_date = '2023-04-12' %}

select
  {{ dbt_utils.generate_surrogate_key(['o.object_id', 'o.version']) }} as unique_key,
  o.object_id,
  o.version,
  o.previous_transaction as tx_digest,
  o.timestamp_ms,
  o.date as block_date,
  cast(date_trunc('month', o.date) as date) as block_month,
  o.checkpoint,
  o.owner_type,
  o.owner_address as receiver,
  o.coin_type,
  o.object_status,
  try_cast(o.coin_balance as bigint) as coin_balance
from {{ source('sui', 'objects') }} o
where o.object_status in ('Created', 'Mutated')
  and o.coin_type is not null
  and o.date >= date '{{ sui_transfer_start_date }}'
  {% if is_incremental() %}
  and {{ incremental_predicate('o.date') }}
  {% endif %}
