{{ config(
    schema = 'momentum_sui',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_digest', 'event_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
) }}

with decoded as (
  select
      -- FollowedSnowflake-style swap fields
      case when cast(json_extract_scalar(event_json, '$.x_for_y') as boolean)
           then cast(json_extract_scalar(event_json, '$.amount_x') as decimal(38,0))
           else cast(json_extract_scalar(event_json, '$.amount_y') as decimal(38,0)) end                as amount_in
      , case when cast(json_extract_scalar(event_json, '$.x_for_y') as boolean)
           then cast(json_extract_scalar(event_json, '$.amount_y') as decimal(38,0))
           else cast(json_extract_scalar(event_json, '$.amount_x') as decimal(38,0)) end                as amount_out
      , cast(json_extract_scalar(event_json, '$.x_for_y') as boolean)                              as a_to_b
      , cast(json_extract_scalar(event_json, '$.fee_amount') as decimal(38,0))                            as fee_amount
      , cast(json_extract_scalar(event_json, '$.protocol_fee') as decimal(38,0))                          as protocol_fee_amount
      , cast(json_extract_scalar(event_json, '$.sqrt_price_after') as double)                      as after_sqrt_price
      , cast(json_extract_scalar(event_json, '$.sqrt_price_before') as double)                     as before_sqrt_price
      , cast(json_extract_scalar(event_json, '$.liquidity') as decimal(38,0))                             as liquidity
      , cast(json_extract_scalar(event_json, '$.reserve_x') as decimal(38,0))                             as reserve_a
      , cast(json_extract_scalar(event_json, '$.reserve_y') as decimal(38,0))                             as reserve_b
      , CAST(cast(json_extract_scalar(event_json, '$.tick_index.bits') as decimal(38,0)) AS BIGINT)       as tick_index_bits

      -- Sui ids & time (Move-native naming)
      , timestamp_ms
      , from_unixtime(timestamp_ms/1000)                                     as block_time
      , date(from_unixtime(timestamp_ms/1000))                               as block_date
      , date_trunc('month', from_unixtime(timestamp_ms/1000))                as block_month
      , transaction_digest
      , event_index
      , epoch
      , checkpoint                                                          -- keep as 'checkpoint'
      , json_extract_scalar(event_json, '$.pool_id')                               as pool_id
      , sender
      , package                                                              as package_address

      -- protocol tag to line up with Snowflake pipeline
      , 'momentum' as protocol
  from {{ source('sui','events') }}
  where event_type = '0x70285592c97965e811e0c6f98dccc3a9c2b4ad854b3594faab9597ada267b860::trade::SwapEvent'
  {% if is_incremental() %}
    and {{ incremental_predicate('from_unixtime(timestamp_ms/1000)') }}
  {% endif %}
)

select
    -- keep Snowflake + Sui naming end-to-end for now
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
    , protocol_fee_amount
    , after_sqrt_price
    , before_sqrt_price
    , liquidity
    , reserve_a
    , reserve_b
    , tick_index_bits
from decoded
where amount_in > 0 and amount_out > 0
