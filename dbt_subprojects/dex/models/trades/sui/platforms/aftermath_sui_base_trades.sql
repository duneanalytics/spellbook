{{ config(
    schema = 'aftermath_sui',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_digest', 'event_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
) }}

{% set aftermath_start_date = "2025-09-13" %}

with decoded as (
  select
      -- core swap fields
      cast(json_extract_scalar(event_json, '$.amounts_in[0]')  as decimal(38,0))  as amount_in
      , cast(json_extract_scalar(event_json, '$.amounts_out[0]') as decimal(38,0))  as amount_out
      , cast(null as boolean) as a_to_b                                    -- no native flag
      , cast(null as decimal(38,0)) as fee_amount
      , cast(null as decimal(38,0)) as protocol_fee_amount
      , cast(null as double) as after_sqrt_price
      , cast(null as double) as before_sqrt_price
      , cast(null as decimal(38,0)) as liquidity
      , cast(null as decimal(38,0)) as reserve_a
      , cast(null as decimal(38,0)) as reserve_b
      , cast(null as bigint) as tick_index_bits
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
      , json_extract_scalar(event_json, '$.issuer')                   as sender
      -- raw coin types
      , json_extract_scalar(event_json, '$.types_in[0]')              as coin_in_raw
      , json_extract_scalar(event_json, '$.types_out[0]')             as coin_out_raw
  from {{ source('sui','events') }}
  where event_type in (
    '0xc4049b2d1cc0f6e017fda8260e4377cecd236bd7f56a54fee120816e72e2e0dd::events::SwapEventV2',
    '0xefe170ec0be4d762196bedecd7a065816576198a6527c99282a2551aaa7da38c::events::SwapEvent'
  )
)

, shaped as (
  select
      timestamp_ms
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
      -- normalized coin types
      , case when coin_in_raw is null then null
       when starts_with(coin_in_raw, '0x') then cast(lower(coin_in_raw) as varbinary)
       else cast(lower(concat('0x', coin_in_raw)) as varbinary) end as coin_type_in
      , case when coin_out_raw is null then null
       when starts_with(coin_out_raw, '0x') then cast(lower(coin_out_raw) as varbinary)
       else cast(lower(concat('0x', coin_out_raw)) as varbinary) end as coin_type_out
  from decoded
)

select
    'sui' as blockchain
    , 'aftermath' as project
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
    , coin_type_in
    , coin_type_out
from shaped
where amount_in > 0
  and amount_out > 0
  and block_time >= timestamp '{{ aftermath_start_date }}'
{% if is_incremental() %}
  and {{ incremental_predicate('block_time') }}
{% endif %}