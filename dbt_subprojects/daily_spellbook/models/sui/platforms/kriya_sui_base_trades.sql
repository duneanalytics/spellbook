{{ config(
    schema = 'kriya_sui',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_digest', 'event_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
) }}

{% set kriya_start_date = "2025-09-14" %}

with decoded as (
  select
      -- swap core
      case when cast(json_extract_scalar(event_json, '$.x_for_y') as boolean)
           then cast(json_extract_scalar(event_json, '$.amount_x') as decimal(38,0))
           else cast(json_extract_scalar(event_json, '$.amount_y') as decimal(38,0))
      end as amount_in
    , case when cast(json_extract_scalar(event_json, '$.x_for_y') as boolean)
           then cast(json_extract_scalar(event_json, '$.amount_y') as decimal(38,0))
           else cast(json_extract_scalar(event_json, '$.amount_x') as decimal(38,0))
      end as amount_out
    , cast(json_extract_scalar(event_json, '$.x_for_y') as boolean)          as a_to_b
    , cast(json_extract_scalar(event_json, '$.fee_amount') as decimal(38,0)) as fee_amount
    , cast(json_extract_scalar(event_json, '$.protocol_fee') as decimal(38,0)) as protocol_fee_amount
    , cast(json_extract_scalar(event_json, '$.sqrt_price_after') as double)  as after_sqrt_price
    , cast(json_extract_scalar(event_json, '$.sqrt_price_before') as double) as before_sqrt_price
    , cast(json_extract_scalar(event_json, '$.liquidity') as decimal(38,0))  as liquidity
    , cast(json_extract_scalar(event_json, '$.reserve_x') as decimal(38,0))  as reserve_a
    , cast(json_extract_scalar(event_json, '$.reserve_y') as decimal(38,0))  as reserve_b
    , cast(json_extract_scalar(event_json, '$.tick_index.bits') as bigint)   as tick_index_bits

      -- ids & time (normalized lowercase strings)
    , timestamp_ms
    , from_unixtime(timestamp_ms/1000)                      as block_time
    , date(from_unixtime(timestamp_ms/1000))                as block_date
    , date_trunc('month', from_unixtime(timestamp_ms/1000)) as block_month
    , ('0x' || lower(to_hex(transaction_digest))) as transaction_digest
    , to_base58(transaction_digest) as transaction_digest_b58
    , event_index
    , epoch
    , checkpoint
    , case
        when json_extract_scalar(event_json, '$.pool_id') is null then null
        when starts_with(lower(json_extract_scalar(event_json, '$.pool_id')), '0x')
          then lower(json_extract_scalar(event_json, '$.pool_id'))
        else concat('0x', lower(json_extract_scalar(event_json, '$.pool_id')))
      end as pool_id
    , case
        when json_extract_scalar(event_json, '$.user') is null then null
        when starts_with(lower(json_extract_scalar(event_json, '$.user')), '0x')
          then lower(json_extract_scalar(event_json, '$.user'))
        else concat('0x', lower(json_extract_scalar(event_json, '$.user')))
      end as sender
  from {{ source('sui','events') }}
  where event_type = '0xf6c05e2d9301e6e91dc6ab6c3ca918f7d55896e1f1edd64adc0e615cde27ebf1::trade::SwapEvent'
    and from_unixtime(timestamp_ms/1000) >= timestamp '{{ kriya_start_date }}'
)

select
    'sui' as blockchain
  , 'kriya' as project
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
  {% if is_incremental() %}
  and {{ incremental_predicate('block_time') }}
  {% endif %}