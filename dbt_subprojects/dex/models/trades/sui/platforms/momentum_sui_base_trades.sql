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
      case when {{ j_bool('event_json', '$.x_for_y') }}
           then {{ j_num('event_json', '$.amount_x') }}
           else {{ j_num('event_json', '$.amount_y') }} end                as amount_in,
      case when {{ j_bool('event_json', '$.x_for_y') }}
           then {{ j_num('event_json', '$.amount_y') }}
           else {{ j_num('event_json', '$.amount_x') }} end                as amount_out,
      {{ j_bool('event_json', '$.x_for_y') }}                              as a_to_b,
      {{ j_num('event_json', '$.fee_amount') }}                            as fee_amount,
      {{ j_num('event_json', '$.protocol_fee') }}                          as protocol_fee_amount,
      {{ j_dbl('event_json', '$.sqrt_price_after') }}                      as after_sqrt_price,
      {{ j_dbl('event_json', '$.sqrt_price_before') }}                     as before_sqrt_price,
      {{ j_num('event_json', '$.liquidity') }}                             as liquidity,
      {{ j_num('event_json', '$.reserve_x') }}                             as reserve_a,
      {{ j_num('event_json', '$.reserve_y') }}                             as reserve_b,
      CAST({{ j_num('event_json', '$.tick_index.bits') }} AS BIGINT)       as tick_index_bits,

      -- Sui ids & time (Move-native naming)
      timestamp_ms,
      from_unixtime(timestamp_ms/1000)                                     as block_time,
      date(from_unixtime(timestamp_ms/1000))                               as block_date,
      date_trunc('month', from_unixtime(timestamp_ms/1000))                as block_month,
      transaction_digest,
      event_index,
      epoch,
      checkpoint,                                                          -- keep as 'checkpoint'
      {{ j_str('event_json', '$.pool_id') }}                               as pool_id,
      sender,
      package                                                              as package_address,

      -- protocol tag to line up with Snowflake pipeline
      'momentum' as protocol
  from {{ source('sui','events') }}
  where event_type = '0x70285592c97965e811e0c6f98dccc3a9c2b4ad854b3594faab9597ada267b860::trade::SwapEvent'
  {% if is_incremental() %}
    and {{ incremental_predicate('from_unixtime(timestamp_ms/1000)') }}
  {% endif %}
)

select
    -- keep Snowflake + Sui naming end-to-end for now
    protocol,
    timestamp_ms,
    block_time,
    block_date,
    block_month,
    transaction_digest,
    event_index,
    epoch,
    checkpoint,
    pool_id,
    sender,
    amount_in,
    amount_out,
    a_to_b,
    fee_amount,
    protocol_fee_amount,
    after_sqrt_price,
    before_sqrt_price,
    liquidity,
    reserve_a,
    reserve_b,
    tick_index_bits
from decoded
where amount_in > 0 and amount_out > 0;
