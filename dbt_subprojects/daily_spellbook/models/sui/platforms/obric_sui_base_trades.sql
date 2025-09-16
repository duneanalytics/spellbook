{{ config(
    schema = 'obric_sui',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_digest', 'event_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
) }}

{% set obric_start_date = "2025-09-14" %}

with decoded as (
  select
      -- core swap fields
      cast(json_extract_scalar(event_json, '$.amount_in')   as decimal(38,0)) as amount_in
    , cast(json_extract_scalar(event_json, '$.amount_out')  as decimal(38,0)) as amount_out
    , cast(json_extract_scalar(event_json, '$.a2b')         as boolean)       as a_to_b

      -- fees & pool state (not emitted by Obric)
    , cast(null as decimal(38,0)) as fee_amount
    , cast(null as decimal(38,0)) as protocol_fee_amount
    , cast(null as double)        as after_sqrt_price
    , cast(null as double)        as before_sqrt_price
    , cast(null as decimal(38,0)) as liquidity
    , cast(null as decimal(38,0)) as reserve_a
    , cast(null as decimal(38,0)) as reserve_b
    , cast(null as bigint)        as tick_index_bits

      -- ids & time (normalized lowercase strings)
    , timestamp_ms
    , from_unixtime(timestamp_ms/1000)                      as block_time
    , date(from_unixtime(timestamp_ms/1000))                as block_date
    , date_trunc('month', from_unixtime(timestamp_ms/1000)) as block_month
    , lower(transaction_digest)                             as transaction_digest
    , event_index
    , epoch
    , checkpoint
    , lower(json_extract_scalar(event_json, '$.pool_id'))   as pool_id
    , lower(sender)                                         as sender
  from {{ source('sui','events') }}
  where event_type = '0x200e762fa2c49f3dc150813038fbf22fd4f894ac6f23ebe1085c62f2ef97f1ca::obric::ObricSwapEvent'
  and block_time >= timestamp '{{ obric_start_date }}'
  {% if is_incremental() %}
  and {{ incremental_predicate('block_time') }}
  {% endif %}
)

select
    'sui' as blockchain
  , 'obric' as project
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
  , cast(null as varchar) as coin_type_in
  , cast(null as varchar) as coin_type_out
from decoded
where amount_in  > 0
  and amount_out > 0