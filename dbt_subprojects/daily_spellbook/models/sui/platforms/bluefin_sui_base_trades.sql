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

{% set bluefin_start_date = "2023-04-12" %}

with base as (
  select
      -- time helpers for partitioning/incremental
      from_unixtime(timestamp_ms/1000)                      as block_time
    , date(from_unixtime(timestamp_ms/1000))                as block_date
    , date_trunc('month', from_unixtime(timestamp_ms/1000)) as block_month

      -- required fields (no casts)
    , json_extract_scalar(event_json, '$.pool_id')   as pool_id
    , cast(json_extract_scalar(event_json,'$.amount_in')  as decimal(38,0)) as amount_in
    , cast(json_extract_scalar(event_json,'$.amount_out') as decimal(38,0)) as amount_out
    , cast(json_extract_scalar(event_json,'$.a2b')        as boolean)       as a_to_b
    , cast(json_extract_scalar(event_json,'$.fee')        as decimal(38,0)) as fee_amount

      -- base ids (native datatypes preserved)
    , timestamp_ms
    , transaction_digest
    , event_index
    , epoch
    , checkpoint
    , sender
  from {{ source('sui','events') }}
  where event_type = '0x3492c874c1e3b3e2984e8c41b589e642d4d0a5d6459e5a9cfc2d52fd7c89c267::events::AssetSwap'
    and from_unixtime(timestamp_ms/1000) >= timestamp '{{ bluefin_start_date }}'
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

    -- trade fields
  , pool_id
  , sender
  , cast(null as varchar) as coin_type_in
  , cast(null as varchar) as coin_type_out
  , amount_in
  , amount_out
  , a_to_b
  , fee_amount

from base
{% if is_incremental() %}
where {{ incremental_predicate('base.block_time') }}
{% endif %}