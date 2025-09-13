{{ config(
    schema = 'bluemove_sui',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_digest', 'event_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
) }}

{% set bluemove_start_date = "2025-09-10" %}

with decoded as (
  select
      -- amounts per side
      cast(json_extract_scalar(event_json, '$.amount_x_in')   as decimal(38,0)) as amount_x_in
      , cast(json_extract_scalar(event_json, '$.amount_y_in')   as decimal(38,0)) as amount_y_in
      , cast(json_extract_scalar(event_json, '$.amount_x_out')  as decimal(38,0)) as amount_x_out
      , cast(json_extract_scalar(event_json, '$.amount_y_out')  as decimal(38,0)) as amount_y_out
      -- raw token type ids
      , json_extract_scalar(event_json, '$.token_x_in')  as token_x_in_raw
      , json_extract_scalar(event_json, '$.token_y_in')  as token_y_in_raw
      , json_extract_scalar(event_json, '$.token_x_out') as token_x_out_raw
      , json_extract_scalar(event_json, '$.token_y_out') as token_y_out_raw
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
      , json_extract_scalar(event_json, '$.user')                     as sender
  from {{ source('sui','events') }}
  where event_type like '0xb24b6789e088b876afabca733bed2299fbc9e2d6369be4d1acfa17d8145454d9::swap::Swap_Event%'
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
      -- infer direction: X->Y if X had input
      , case when amount_x_in > 0 then true else false end as a_to_b
      -- core amounts by direction
      , case when amount_x_in  > 0 then amount_x_in  else amount_y_in  end as amount_in
      , case when amount_x_out > 0 then amount_x_out else amount_y_out end as amount_out
      -- normalize token type ids
      , case when token_x_in_raw  is null then null
       when starts_with(token_x_in_raw,  '0x') then cast(lower(token_x_in_raw) as varbinary)
       else cast(lower(concat('0x', token_x_in_raw)) as varbinary) end as token_x_in_norm
      , case when token_y_in_raw  is null then null
       when starts_with(token_y_in_raw,  '0x') then cast(lower(token_y_in_raw) as varbinary)
       else cast(lower(concat('0x', token_y_in_raw)) as varbinary) end as token_y_in_norm
      , case when token_x_out_raw is null then null
       when starts_with(token_x_out_raw, '0x') then cast(lower(token_x_out_raw) as varbinary)
       else cast(lower(concat('0x', token_x_out_raw)) as varbinary) end as token_x_out_norm
      , case when token_y_out_raw is null then 
      null
       when starts_with(token_y_out_raw, '0x') then cast(lower(token_y_out_raw) as varbinary)
       else cast(lower(concat('0x', token_y_out_raw)) as varbinary) end as token_y_out_norm
  from decoded
)

select
    'sui' as blockchain
    , 'bluemove' as project
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
    , case when a_to_b then token_x_in_norm  else token_y_in_norm  end as coin_type_in
    , case when a_to_b then token_y_out_norm else token_x_out_norm end as coin_type_out
from shaped
where amount_in > 0
  and amount_out > 0
  and block_time >= timestamp '{{ bluemove_start_date }}'
{% if is_incremental() %}
  and {{ incremental_predicate('block_time') }}
{% endif %}