{{ config(
    schema = 'deepbook_sui',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_digest', 'event_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
) }}

{% set deepbook_start_date = "2024-10-01" %}

with base as (
  select
      from_unixtime(timestamp_ms/1000)                      as block_time
    , date(from_unixtime(timestamp_ms/1000))                as block_date
    , date_trunc('month', from_unixtime(timestamp_ms/1000)) as block_month

    , json_extract_scalar(event_json, '$.pool_id')                     as pool_id
    , cast(json_extract_scalar(event_json, '$.base_quantity')  as decimal(38,0)) as base_quantity
    , cast(json_extract_scalar(event_json, '$.quote_quantity') as decimal(38,0)) as quote_quantity
    , cast(json_extract_scalar(event_json, '$.price')          as decimal(38,0)) as price
    , cast(json_extract_scalar(event_json, '$.taker_is_bid')   as boolean)       as taker_is_bid
    , cast(json_extract_scalar(event_json, '$.taker_fee')      as decimal(38,0)) as taker_fee
    , cast(json_extract_scalar(event_json, '$.taker_fee_is_deep') as boolean)    as taker_fee_is_deep
    , json_extract_scalar(event_json, '$.maker_balance_manager_id')               as maker_id
    , json_extract_scalar(event_json, '$.taker_balance_manager_id')               as taker_id

    , timestamp_ms
    , transaction_digest
    , event_index
    , epoch
    , checkpoint
    , sender
  from {{ source('sui','events') }}
  where event_type = '0x2c8d603bc51326b8c13cef9dd07031a408a48dddb541963357661df5d3204809::order_info::OrderFilled'
    and from_unixtime(timestamp_ms/1000) >= timestamp '{{ deepbook_start_date }}'
)

select
    -- ids & time
    timestamp_ms
  , transaction_digest
  , event_index
  , epoch
  , checkpoint
  , block_time
  , block_date
  , block_month

  , pool_id
  , taker_id as sender  
  , cast(null as varchar) as coin_type_in
  , cast(null as varchar) as coin_type_out
  , case
      when taker_is_bid then quote_quantity
      else base_quantity
    end as amount_in
  , case
      when taker_is_bid then base_quantity
      else quote_quantity
    end as amount_out
  , not taker_is_bid as a_to_b
  , taker_fee as fee_amount 

from base
{% if is_incremental() %}
where {{ incremental_predicate('base.block_time') }}
{% endif %}