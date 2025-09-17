{{ config(
    schema = 'aftermath_sui',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_digest', 'event_index'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('block_time')]
) }}

{% set aftermath_start_date = "2025-09-14" %}

with base as (
  select
      -- time helpers for partitioning/incremental (derived; doesn’t change base types)
      from_unixtime(timestamp_ms/1000)                      as block_time
    , date(from_unixtime(timestamp_ms/1000))                as block_date
    , date_trunc('month', from_unixtime(timestamp_ms/1000)) as block_month

      -- required fields
    , json_extract_scalar(event_json, '$.pool_id')          as pool_id
    , cast(json_extract_scalar(event_json,'$.amounts_in[0]')  as decimal(38,0)) as amount_in
    , cast(json_extract_scalar(event_json,'$.amounts_out[0]') as decimal(38,0)) as amount_out
    , json_extract_scalar(event_json, '$.types_in[0]')      as coin_type_in
    , json_extract_scalar(event_json, '$.types_out[0]')     as coin_type_out
    , sender                                                -- base column (preserve type)
    , timestamp_ms
    , transaction_digest
    , event_index
    , epoch
    , checkpoint
  from {{ source('sui','events') }}
  where event_type in (
    '0xc4049b2d1cc0f6e017fda8260e4377cecd236bd7f56a54fee120816e72e2e0dd::events::SwapEventV2',
    '0xefe170ec0be4d762196bedecd7a065816576198a6527c99282a2551aaa7da38c::events::SwapEvent'
  )
  and from_unixtime(timestamp_ms/1000) >= timestamp '{{ aftermath_start_date }}'
)

select
    -- core output
    timestamp_ms
  , transaction_digest
  , event_index
  , epoch
  , checkpoint
  , pool_id
  , sender
  , coin_type_in
  , coin_type_out
  , amount_in
  , amount_out

  -- placeholders (not in Aftermath events)
  , cast(null as boolean) as a_to_b
  , cast(null as decimal(38,0)) as fee_amount

  , block_time
  , block_date
  , block_month
from base
{% if is_incremental() %}
where {{ incremental_predicate('block_time') }}
{% endif %}
;
