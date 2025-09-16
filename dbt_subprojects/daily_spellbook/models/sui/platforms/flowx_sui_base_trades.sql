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

{% set flowx_start_date = "2025-09-14" %}

with decoded as (
  select
      -- amounts per side
      cast(json_extract_scalar(event_json, '$.amount_x_in')   as decimal(38,0)) as amount_x_in
    , cast(json_extract_scalar(event_json, '$.amount_y_in')   as decimal(38,0)) as amount_y_in
    , cast(json_extract_scalar(event_json, '$.amount_x_out')  as decimal(38,0)) as amount_x_out
    , cast(json_extract_scalar(event_json, '$.amount_y_out')  as decimal(38,0)) as amount_y_out

      -- raw coin ids
    , json_extract_scalar(event_json, '$.coin_x') as coin_x_raw
    , json_extract_scalar(event_json, '$.coin_y') as coin_y_raw

      -- ids & time (normalized lowercase strings)
    , timestamp_ms
    , from_unixtime(timestamp_ms/1000)                      as block_time
    , date(from_unixtime(timestamp_ms/1000))                as block_date
    , date_trunc('month', from_unixtime(timestamp_ms/1000)) as block_month
    , lower(transaction_digest)                             as transaction_digest
    , event_index
    , epoch
    , checkpoint
    , cast(null as varchar)                                 as pool_id  -- not provided in event
    , lower(json_extract_scalar(event_json, '$.user'))      as sender
  from {{ source('sui','events') }}
  where event_type = '0xba153169476e8c3114962261d1edc70de5ad9781b83cc617ecc8c1923191cae0::pair::Swapped'
    and from_unixtime(timestamp_ms/1000) >= timestamp '{{ flowx_start_date }}'
),

shaped as (
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

      -- direction inferred: X provided input means a_to_b
    , case when amount_x_in > 0 then true else false end as a_to_b

      -- core amounts by direction
    , case when amount_x_in  > 0 then amount_x_in  else amount_y_in  end as amount_in
    , case when amount_x_out > 0 then amount_x_out else amount_y_out end as amount_out

      -- normalized coin ids (native VARCHAR; lowercase; ensure 0x prefix)
    , case
        when coin_x_raw is null then null
        when starts_with(coin_x_raw, '0x') then lower(coin_x_raw)
        else lower(concat('0x', coin_x_raw))
      end as coin_x_norm
    , case
        when coin_y_raw is null then null
        when starts_with(coin_y_raw, '0x') then lower(coin_y_raw)
        else lower(concat('0x', coin_y_raw))
      end as coin_y_norm
  from decoded
)

select
    'sui' as blockchain
  , 'flowx' as project
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
  , cast(null as decimal(38,0)) as fee_amount
  , cast(null as decimal(38,0)) as protocol_fee_amount
  , cast(null as double)        as after_sqrt_price
  , cast(null as double)        as before_sqrt_price
  , cast(null as decimal(38,0)) as liquidity
  , cast(null as decimal(38,0)) as reserve_a
  , cast(null as decimal(38,0)) as reserve_b
  , cast(null as bigint)        as tick_index_bits
  , case when a_to_b then coin_x_norm else coin_y_norm end as coin_type_in
  , case when a_to_b then coin_y_norm else coin_x_norm end as coin_type_out
from shaped
where amount_in  > 0
  and amount_out > 0
  {% if is_incremental() %}
  and {{ incremental_predicate('block_time') }}
  {% endif %}