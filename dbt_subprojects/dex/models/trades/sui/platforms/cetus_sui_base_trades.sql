{{ config(
    schema = 'cetus_sui',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_digest', 'event_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
) }}

with decoded as (
  select
      -- core swap fields (match Snowflake)
      cast(json_extract_scalar(event_json, '$.amount_in')         as decimal(38,0))  as amount_in
      , cast(json_extract_scalar(event_json, '$.amount_out')        as decimal(38,0))  as amount_out
      , cast(json_extract_scalar(event_json, '$.atob')              as boolean)        as a_to_b
      , cast(json_extract_scalar(event_json, '$.fee_amount')        as decimal(38,0))  as fee_amount
      , cast(json_extract_scalar(event_json, '$.after_sqrt_price')  as double)         as after_sqrt_price
      , cast(json_extract_scalar(event_json, '$.before_sqrt_price') as double)         as before_sqrt_price

      -- ids & time
      , timestamp_ms
      , from_unixtime(timestamp_ms/1000)                              as block_time
      , date(from_unixtime(timestamp_ms/1000))                        as block_date
      , date_trunc('month', from_unixtime(timestamp_ms/1000))         as block_month
      , transaction_digest
      , event_index
      , epoch
      , checkpoint
      , json_extract_scalar(event_json, '$.pool')                     as pool_id
      , sender

      , 'cetus' as protocol
  from {{ source('sui','events') }}
  where event_type = '0x1eabed72c53feb3805120a081dc15963c204dc8d091542592abaf7a35689b2fb::pool::SwapEvent'
  {% if is_incremental() %}
    and {{ incremental_predicate('from_unixtime(timestamp_ms/1000)') }}
  {% endif %}
)

select
    protocol
    , timestamp_ms
    , block_time
    , block_date
    , block_month
    , transaction_digest
    , event_index
    , epoch
    , checkpoint
    , pool_id
    , sender
    , amount_in
    , amount_out
    , a_to_b
    , fee_amount
    , cast(null as decimal(38,0)) as protocol_fee_amount -- Cetus doesn't emit this
    , after_sqrt_price
    , before_sqrt_price
    -- placeholders to keep the union schema stable
    , cast(null as decimal(38,0)) as liquidity
    , cast(null as decimal(38,0)) as reserve_a
    , cast(null as decimal(38,0)) as reserve_b
    , cast(null as bigint)        as tick_index_bits
from decoded
where amount_in > 0
  and amount_out > 0;
