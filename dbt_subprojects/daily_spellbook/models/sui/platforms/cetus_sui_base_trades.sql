{{ config(
    schema = 'cetus_sui',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_digest', 'event_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
) }}

{% set cetus_start_date = "2025-09-14" %}

with decoded as (
  select
      -- core swap fields
      cast(json_extract_scalar(event_json, '$.amount_in')          as decimal(38,0)) as amount_in
    , cast(json_extract_scalar(event_json, '$.amount_out')         as decimal(38,0)) as amount_out
    , cast(json_extract_scalar(event_json, '$.atob')               as boolean)       as a_to_b
    , cast(json_extract_scalar(event_json, '$.fee_amount')         as decimal(38,0)) as fee_amount
    , cast(json_extract_scalar(event_json, '$.after_sqrt_price')   as double)        as after_sqrt_price
    , cast(json_extract_scalar(event_json, '$.before_sqrt_price')  as double)        as before_sqrt_price

      -- ids & time (normalized lowercase strings)
    , timestamp_ms
    , from_unixtime(timestamp_ms/1000)                      as block_time
    , date(from_unixtime(timestamp_ms/1000))                as block_date
    , date_trunc('month', from_unixtime(timestamp_ms/1000)) as block_month
    , ('0x' || lower(to_hex(from_base58(transaction_digest)))) as transaction_digest
    , transaction_digest as transaction_digest_b58
    , event_index
    , epoch
    , checkpoint
    , case
        when json_extract_scalar(event_json, '$.pool') is null then null
        when starts_with(lower(json_extract_scalar(event_json, '$.pool')), '0x')
          then lower(json_extract_scalar(event_json, '$.pool'))
        else concat('0x', lower(json_extract_scalar(event_json, '$.pool')))
      end as pool_id
    , case
        when json_extract_scalar(event_json, '$.user') is null then null
        when starts_with(lower(json_extract_scalar(event_json, '$.user')), '0x')
          then lower(json_extract_scalar(event_json, '$.user'))
        else concat('0x', lower(json_extract_scalar(event_json, '$.user')))
      end as sender
  from {{ source('sui','events') }}
    where event_type = '0x1eabed72c53feb3805120a081dc15963c204dc8d091542592abaf7a35689b2fb::pool::SwapEvent'
    and from_unixtime(timestamp_ms/1000) >= timestamp '{{ cetus_start_date }}'
)

select
    'sui' as blockchain
  , 'cetus' as project
  , '1' as version
  , timestamp_ms
  , block_time
  , block_date
  , block_month
  , transaction_digest
  , transaction_digest_b58
  , event_index
  , epoch
  , checkpoint
  , pool_id
  , sender
  , amount_in
  , amount_out
  , a_to_b
  , fee_amount
  , cast(null as decimal(38,0)) as protocol_fee_amount
  , after_sqrt_price
  , before_sqrt_price

    -- placeholders to keep the union schema stable
  , cast(null as decimal(38,0)) as liquidity
  , cast(null as decimal(38,0)) as reserve_a
  , cast(null as decimal(38,0)) as reserve_b
  , cast(null as bigint)        as tick_index_bits

    -- Cetus events don't emit coin types; keep as native VARCHAR nulls
  , cast(null as varchar)       as coin_type_in
  , cast(null as varchar)       as coin_type_out
from decoded
where amount_in  > 0
  and amount_out > 0
  {% if is_incremental() %}
  and {{ incremental_predicate('block_time') }}
  {% endif %}