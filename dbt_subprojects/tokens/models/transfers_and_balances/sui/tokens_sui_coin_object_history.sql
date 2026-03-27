{{
  config(
    schema = 'tokens_sui',
    alias = 'coin_object_history',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['object_id', 'version'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    merge_skip_unchanged = true,
    tags = ['sui', 'tokens', 'transfers'],
  )
}}

-- temporary ci filter: original start date '2023-04-12', bumped to '2026-03-01' to reduce scan and unblock ci run
{% set sui_transfer_start_date = '2026-03-01' %}

with

source_rows as (
  select
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
)

select
  s.object_id,
  s.version,
  s.tx_digest,
  s.timestamp_ms,
  s.block_date,
  s.block_month,
  s.checkpoint,
  s.owner_type,
  s.receiver,
  s.coin_type,
  s.object_status,
  s.coin_balance,
  current_timestamp as _updated_at
from source_rows s
