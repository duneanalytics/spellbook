{{ config(
    schema = 'bluemove_sui',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_digest', 'event_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
) }}

{% set bluemove_start_date = "2025-09-17" %}

with decoded as (
  select
      -- time helpers for partition/incremental
      from_unixtime(timestamp_ms/1000)                      as block_time
    , date(from_unixtime(timestamp_ms/1000))                as block_date
    , date_trunc('month', from_unixtime(timestamp_ms/1000)) as block_month

      -- raw amounts per side (cast JSON only to compute direction/selection)
    , cast(json_extract_scalar(event_json, '$.amount_x_in')  as decimal(38,0)) as amount_x_in
    , cast(json_extract_scalar(event_json, '$.amount_y_in')  as decimal(38,0)) as amount_y_in
    , cast(json_extract_scalar(event_json, '$.amount_x_out') as decimal(38,0)) as amount_x_out
    , cast(json_extract_scalar(event_json, '$.amount_y_out') as decimal(38,0)) as amount_y_out

      -- token ids (no normalization)
    , json_extract_scalar(event_json, '$.token_x_in')   as token_x_in
    , json_extract_scalar(event_json, '$.token_y_in')   as token_y_in
    , json_extract_scalar(event_json, '$.token_x_out')  as token_x_out
    , json_extract_scalar(event_json, '$.token_y_out')  as token_y_out

      -- ids (keep base columns untouched where available)
    , json_extract_scalar(event_json, '$.pool_id') as pool_id
    , sender
    , timestamp_ms
    , transaction_digest
    , event_index
    , epoch
    , checkpoint
  from {{ source('sui','events') }}
  where event_type like '0xb24b6789e088b876afabca733bed2299fbc9e2d6369be4d1acfa17d8145454d9::swap::Swap_Event%'
    and from_unixtime(timestamp_ms/1000) >= timestamp '{{ bluemove_start_date }}'
),

shaped as (
  select
      -- ids & time
      timestamp_ms, transaction_digest, event_index, epoch, checkpoint,
      block_time, block_date, block_month,
      pool_id,
      sender,

      -- direction: X->Y if X had input
      case when amount_x_in > 0 then true else false end as a_to_b,

      -- amounts by direction
      case when amount_x_in  > 0 then amount_x_in  else amount_y_in  end as amount_in,
      case when amount_x_out > 0 then amount_x_out else amount_y_out end as amount_out,

      -- coin types by direction (no normalization)
      case when amount_x_in > 0 then token_x_in  else token_y_in  end as coin_type_in,
      case when amount_x_in > 0 then token_y_out else token_x_out end as coin_type_out

  from decoded
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
  , cast(null as decimal(38,0)) as fee_amount    -- Bluemove swap event doesn’t emit fee here

from shaped
{% if is_incremental() %}
where {{ incremental_predicate('shaped.block_time') }}
{% endif %}
