{{
  config(
    schema = 'tokens_sui',
    alias = 'object_state',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['object_id'],
    merge_skip_unchanged = true,
    tags = ['sui', 'tokens', 'transfers'],
  )
}}

{% set sui_transfer_start_date = '2023-04-12' %}

with

source_rows as (
  select
    o.object_id,
    o.version,
    o.timestamp_ms,
    o.date as block_date,
    cast(date_trunc('month', o.date) as date) as block_month,
    o.checkpoint,
    o.owner_type,
    o.owner_address as receiver,
    o.coin_type,
    try_cast(o.coin_balance as bigint) as coin_balance
  from {{ source('sui', 'objects') }} o
  where o.object_status in ('Created', 'Mutated')
    and o.coin_type is not null
    and o.date >= date '{{ sui_transfer_start_date }}'
    {% if is_incremental() -%}
    and {{ incremental_predicate('o.date') }}
    {% endif -%}
),

latest_incremental as (
  select
    s.object_id,
    s.version,
    s.timestamp_ms,
    s.block_date,
    s.block_month,
    s.checkpoint,
    s.owner_type,
    s.receiver,
    s.coin_type,
    s.coin_balance
  from (
    select
      s.*,
      row_number() over (partition by s.object_id order by s.version desc) as rn
    from source_rows s
  ) s
  where s.rn = 1
),

existing_state as (
  {% if is_incremental() -%}
  select
    e.object_id,
    e.version,
    e.timestamp_ms,
    e.block_date,
    e.block_month,
    e.checkpoint,
    e.owner_type,
    e.receiver,
    e.coin_type,
    e.coin_balance
  from {{ this }} e
  {% else -%}
  select
    cast(null as varbinary) as object_id,
    cast(null as bigint) as version,
    cast(null as bigint) as timestamp_ms,
    cast(null as date) as block_date,
    cast(null as date) as block_month,
    cast(null as bigint) as checkpoint,
    cast(null as varchar) as owner_type,
    cast(null as varbinary) as receiver,
    cast(null as varchar) as coin_type,
    cast(null as bigint) as coin_balance
  where false
  {% endif -%}
)

select
  s.object_id,
  s.version,
  s.timestamp_ms,
  s.block_date,
  s.block_month,
  s.checkpoint,
  s.owner_type,
  s.receiver,
  s.coin_type,
  s.coin_balance,
  e.version as previous_version,
  e.timestamp_ms as previous_timestamp_ms,
  e.block_date as previous_block_date,
  e.block_month as previous_block_month,
  e.checkpoint as previous_checkpoint,
  e.owner_type as previous_owner_type,
  e.receiver as previous_receiver,
  e.coin_type as previous_coin_type,
  e.coin_balance as previous_coin_balance,
  current_timestamp as _updated_at
from latest_incremental s
left join existing_state e
  on s.object_id = e.object_id
