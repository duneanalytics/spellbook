{{ config(
    schema = 'cetus_sui',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_digest', 'event_index'],
    incremental_predicates = [incremental_predicate('block_time')]
) }}

{% set cetus_start_date = "2025-09-14" %}

with base as (
  select
      -- time helpers for partitioning/incremental
      from_unixtime(timestamp_ms/1000)                      as block_time
    , date(from_unixtime(timestamp_ms/1000))                as block_date
    , date_trunc('month', from_unixtime(timestamp_ms/1000)) as block_month

      -- minimal fields from event_json (no casts, no normalization)
    , json_extract_scalar(event_json, '$.pool')        as pool_id
    , cast(json_extract_scalar(event_json,'$.amount_in')    as decimal(38,0)) as amount_in
    , cast(json_extract_scalar(event_json,'$.amount_out')   as decimal(38,0)) as amount_out
    , cast(json_extract_scalar(event_json,'$.atob')         as boolean)       as a_to_b
    , cast(json_extract_scalar(event_json,'$.fee_amount')   as decimal(38,0)) as fee_amount

      -- base ids (preserve native datatypes)
    , timestamp_ms
    , transaction_digest
    , event_index
    , epoch
    , checkpoint
    , sender
  from {{ source('sui','events') }}
  where event_type = '0x1eabed72c53feb3805120a081dc15963c204dc8d091542592abaf7a35689b2fb::pool::SwapEvent'
    and from_unixtime(timestamp_ms/1000) >= timestamp '{{ cetus_start_date }}'
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
  , cast(null as varchar) as coin_type_in -- Cetus event doesn't emit coin types
  , cast(null as varchar) as coin_type_out
  , amount_in
  , amount_out
  , a_to_b
  , fee_amount

from base
{% if is_incremental() %}
where {{ incremental_predicate('block_time') }}
{% endif %}