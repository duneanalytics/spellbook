{{ config(
    schema = 'flowx_sui',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_digest', 'event_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
) }}

{% set flowx_start_date = "2025-09-17" %}

with decoded as (
  select
      -- time helpers for partition/incremental
      from_unixtime(timestamp_ms/1000)                      as block_time
    , date(from_unixtime(timestamp_ms/1000))                as block_date
    , date_trunc('month', from_unixtime(timestamp_ms/1000)) as block_month

      -- amounts per side (cast JSON only as needed)
    , cast(json_extract_scalar(event_json, '$.amount_x_in')   as decimal(38,0)) as amount_x_in
    , cast(json_extract_scalar(event_json, '$.amount_y_in')   as decimal(38,0)) as amount_y_in
    , cast(json_extract_scalar(event_json, '$.amount_x_out')  as decimal(38,0)) as amount_x_out
    , cast(json_extract_scalar(event_json, '$.amount_y_out')  as decimal(38,0)) as amount_y_out

      -- coin ids (no normalization)
    , json_extract_scalar(event_json, '$.coin_x') as coin_x
    , json_extract_scalar(event_json, '$.coin_y') as coin_y

      -- ids (keep base columns untouched)
    , timestamp_ms
    , transaction_digest
    , event_index
    , epoch
    , checkpoint
    , sender
    , cast(null as varchar) as pool_id  -- not provided by FlowX event
  from {{ source('sui','events') }}
  where event_type = '0xba153169476e8c3114962261d1edc70de5ad9781b83cc617ecc8c1923191cae0::pair::Swapped'
    and from_unixtime(timestamp_ms/1000) >= timestamp '{{ flowx_start_date }}'
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
      case when amount_x_in > 0 then coin_x else coin_y end as coin_type_in,
      case when amount_x_in > 0 then coin_y else coin_x end as coin_type_out

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
  , cast(null as decimal(38,0)) as fee_amount   -- FlowX event does not emit fee

from shaped
{% if is_incremental() %}
where {{ incremental_predicate('shaped.block_time') }}
{% endif %}