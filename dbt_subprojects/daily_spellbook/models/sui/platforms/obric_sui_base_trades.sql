{{ config(
    schema = 'obric_sui',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_digest', 'event_index'],
    incremental_predicates = [incremental_predicate('block_time')]
) }}

{% set obric_start_date = "2025-09-14" %}

with base as (
  select
      -- time helpers for partitioning/incremental
      from_unixtime(timestamp_ms/1000)                      as block_time
    , date(from_unixtime(timestamp_ms/1000))                as block_date
    , date_trunc('month', from_unixtime(timestamp_ms/1000)) as block_month

      -- minimal fields from event_json (no normalization)
    , json_extract_scalar(event_json, '$.pool_id')          as pool_id
    , cast(json_extract_scalar(event_json,'$.amount_in')  as decimal(38,0)) as amount_in,
    , cast(json_extract_scalar(event_json,'$.amount_out') as decimal(38,0)) as amount_out,
    , cast(json_extract_scalar(event_json, '$.a2b') as boolean) as a_to_b
    , json_extract_scalar(event_json, '$.coin_a.name')      as coin_a
    , json_extract_scalar(event_json, '$.coin_b.name')      as coin_b

      -- base ids (preserve native datatypes)
    , timestamp_ms
    , transaction_digest
    , event_index
    , epoch
    , checkpoint
    , sender
  from {{ source('sui','events') }}
  where event_type = '0x200e762fa2c49f3dc150813038fbf22fd4f894ac6f23ebe1085c62f2ef97f1ca::obric::ObricSwapEvent'
    and from_unixtime(timestamp_ms/1000) >= timestamp '{{ obric_start_date }}'
),

shaped as (
  select
      -- ids & time
      timestamp_ms, transaction_digest, event_index, epoch, checkpoint,
      block_time, block_date, block_month,

      -- trade identifiers
      pool_id,
      sender,

      -- direction & amounts (amounts are provided directly in the event)
      amount_in,
      amount_out,
      a_to_b,

      -- coin types chosen by direction (no normalization)
      case when a_to_b then coin_a else coin_b end as coin_type_in,
      case when a_to_b then coin_b else coin_a end as coin_type_out

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
  , cast(null as decimal(38,0)) as fee_amount   -- This event was not provided directly

from shaped
{% if is_incremental() %}
where {{ incremental_predicate('block_time') }}
{% endif %}