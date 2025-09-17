{{ config(
    schema = 'kriya_sui',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_digest', 'event_index'],
    incremental_predicates = [incremental_predicate('block_time')]
) }}

{% set kriya_start_date = "2025-09-14" %}

with base as (
  select
      -- time helpers for partitioning/incremental
      from_unixtime(timestamp_ms/1000)                      as block_time
    , date(from_unixtime(timestamp_ms/1000))                as block_date
    , date_trunc('month', from_unixtime(timestamp_ms/1000)) as block_month

      -- minimal fields from event_json (cast JSON only; no normalization)
    , json_extract_scalar(event_json, '$.pool_id')           as pool_id
    , cast(json_extract_scalar(event_json, '$.x_for_y') as boolean)          as a_to_b
    , cast(json_extract_scalar(event_json, '$.amount_x') as decimal(38,0))   as amount_x
    , cast(json_extract_scalar(event_json, '$.amount_y') as decimal(38,0))   as amount_y
    , cast(json_extract_scalar(event_json, '$.fee_amount') as decimal(38,0)) as fee_amount

      -- base ids (preserve native datatypes)
    , timestamp_ms
    , transaction_digest
    , event_index
    , epoch
    , checkpoint
    , sender
  from {{ source('sui','events') }}
  where event_type = '0xf6c05e2d9301e6e91dc6ab6c3ca918f7d55896e1f1edd64adc0e615cde27ebf1::trade::SwapEvent'
    and from_unixtime(timestamp_ms/1000) >= timestamp '{{ kriya_start_date }}'
),

shaped as (
  select
      -- ids & time
      timestamp_ms, transaction_digest, event_index, epoch, checkpoint,
      block_time, block_date, block_month,

      -- trade identifiers
      pool_id,
      sender,

      -- amounts selected by direction
      case when a_to_b then amount_x else amount_y end as amount_in,
      case when a_to_b then amount_y else amount_x end as amount_out,

      -- direction & fee
      a_to_b,
      fee_amount,

      -- coin types not emitted in this event
      null as coin_type_in,
      null as coin_type_out
  from base
)

select
    -- ids & time
    timestamp_ms
  , transaction_digest
  , event_index
  , epoch
  , checkpoint
  , block_time
  , block_date
  , block_month

  -- trade fields
  , pool_id
  , sender
  , coin_type_in
  , coin_type_out
  , amount_in
  , amount_out
  , a_to_b
  , fee_amount

from shaped
{% if is_incremental() %}
where {{ incremental_predicate('block_time') }}
{% endif %}