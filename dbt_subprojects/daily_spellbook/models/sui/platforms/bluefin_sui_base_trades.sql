{{ config(
    schema = 'bluefin_sui',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_digest', 'event_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
) }}

{% set bluefin_start_date = "2025-09-13" %}

with decoded as (
  select
      -- core swap fields
      cast(json_extract_scalar(event_json, '$.amount_in')   as decimal(38,0))  as amount_in
      , cast(json_extract_scalar(event_json, '$.amount_out')  as decimal(38,0))  as amount_out
      , cast(json_extract_scalar(event_json, '$.a2b')         as boolean)        as a_to_b
      , cast(json_extract_scalar(event_json, '$.fee')         as decimal(38,0))  as fee_amount
      , cast(null as decimal(38,0)) as protocol_fee_amount
      , cast(json_extract_scalar(event_json, '$.after_sqrt_price')  as double)   as after_sqrt_price
      , cast(json_extract_scalar(event_json, '$.before_sqrt_price') as double)   as before_sqrt_price
      , cast(null as decimal(38,0)) as liquidity
      , cast(null as decimal(38,0)) as reserve_a
      , cast(null as decimal(38,0)) as reserve_b
      , cast(json_extract_scalar(event_json, '$.current_tick.bits') as bigint)   as tick_index_bits
      -- ids & time
      , timestamp_ms
      , from_unixtime(timestamp_ms/1000)                              as block_time
      , date(from_unixtime(timestamp_ms/1000))                        as block_date
      , date_trunc('month', from_unixtime(timestamp_ms/1000))         as block_month
      , transaction_digest
      , event_index
      , epoch
      , checkpoint
      , json_extract_scalar(event_json, '$.pool_id')                  as pool_id
      , sender
  from {{ source('sui','events') }}
  where event_type = '0x3492c874c1e3b3e2984e8c41b589e642d4d0a5d6459e5a9cfc2d52fd7c89c267::events::AssetSwap'
)

select
    'sui' as blockchain
    , 'bluefin' as project
    , '1' as version
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
    , protocol_fee_amount
    , after_sqrt_price
    , before_sqrt_price
    , liquidity
    , reserve_a
    , reserve_b
    , tick_index_bits
    , cast(null as varbinary)       as coin_type_in -- not emitted in event
    , cast(null as varbinary)       as coin_type_out
from decoded
where amount_in > 0
  and amount_out > 0
  and block_time >= timestamp '{{ bluefin_start_date }}'
{% if is_incremental() %}
  and {{ incremental_predicate('block_time') }}
{% endif %}