{{ config(
    schema = 'sui_walrus',
    alias = 'base_table',
    materialized = 'incremental',
    file_format = 'delta',
    partition_by = ['block_month'],
    incremental_strategy = 'merge',
    unique_key = ['tx_register','evt_index_register'],
    incremental_predicates = [ incremental_predicate('DBT_INTERNAL_DEST.block_date') ]
) }}

{% set walrus_start_date = var('walrus_start_date', '2025-09-13') %}

-- 1) Walrus events (package + event types)
with events as (
  select
      sender,
      epoch                                                  as sui_epoch,
      cast(json_extract_scalar(event_json, '$.epoch') as bigint) as epoch,
      event_type,
      event_json,
      transaction_digest,
      event_index,
      timestamp_ms,
      from_unixtime(timestamp_ms/1000)                          as block_time,
      date(from_unixtime(timestamp_ms/1000))                    as block_date,
      date_trunc('month', from_unixtime(timestamp_ms/1000))     as block_month
  from {{ source('sui','events') }}
  where event_type in (
    '0xfdc88f7d7cf30afab2f82e8380d11ee8f70efb90e863d1de8616fae1bb09ea77::events::BlobRegistered',
    '0xfdc88f7d7cf30afab2f82e8380d11ee8f70efb90e863d1de8616fae1bb09ea77::events::BlobCertified'
  )

  {% if not is_incremental() %}
    and from_unixtime(timestamp_ms/1000) >= timestamp '{{ walrus_start_date }}'
  {% endif %}
),

-- 2) Normalize payloads (match Snowflake names/logic)
event_data as (
  select
      transaction_digest,
      event_index,
      timestamp_ms,
      sender,
      sui_epoch,
      epoch,
      case
        when event_type = '0xfdc88f7d7cf30afab2f82e8380d11ee8f70efb90e863d1de8616fae1bb09ea77::events::BlobRegistered'
          then 'register' else 'certify' end as action,

      -- Snowflake does: '0x' || REPLACE(event_json:blob_id, '"','')
      case
        when json_extract_scalar(event_json, '$.blob_id') is null then null
        when starts_with(json_extract_scalar(event_json, '$.blob_id'), '0x')
          then lower(json_extract_scalar(event_json, '$.blob_id'))
        else concat('0x', lower(json_extract_scalar(event_json, '$.blob_id')))
      end as blob_id,

      -- Snowflake keeps object_id as-is (no 0x prefix), just strips quotes
      lower(json_extract_scalar(event_json, '$.object_id'))       as object_id,

      cast(json_extract_scalar(event_json, '$.deletable') as boolean) as deletable,
      cast(json_extract_scalar(event_json, '$.epoch')     as bigint)  as starting_epoch,
      cast(json_extract_scalar(event_json, '$.end_epoch') as bigint)  as ending_epoch,

      case
        when event_type = '0xfdc88f7d7cf30afab2f82e8380d11ee8f70efb90e863d1de8616fae1bb09ea77::events::BlobRegistered'
          then cast(json_extract_scalar(event_json, '$.size') as decimal(38,0))
        else null end as size,

      case
        when event_type = '0xfdc88f7d7cf30afab2f82e8380d11ee8f70efb90e863d1de8616fae1bb09ea77::events::BlobCertified'
          then cast(json_extract_scalar(event_json, '$.is_extension') as boolean)
        else null end as extension,

      -- tech columns retained for partitioning/merge
      from_unixtime(timestamp_ms/1000)                          as block_time,
      date(from_unixtime(timestamp_ms/1000))                    as block_date,
      date_trunc('month', from_unixtime(timestamp_ms/1000))     as block_month
  from events
),

-- 3) Split and join register ↔ certify (exact Snowflake join keys)
dataset as (
  select
    l.sender,
    cast(null as varchar) as partner_name,
    l.epoch         as epoch_register,

    -- human-readable register time
    l.block_time    as ts_register,

    l.blob_id       as blob_hash,
    l.object_id,
    l.deletable,
    l.starting_epoch,
    l.ending_epoch,

    case when l.size is not null
         then cast(l.size as decimal(38,6)) / cast(1000000 as decimal(38,6))
         else null end as size_mb,

    r.extension,
    case when r.sender is null then 0 else 1 end as certify,

    -- human-readable certify time
    r.block_time    as ts_certify,

    r.epoch         as epoch_certify,
    l.transaction_digest as tx_register,
    l.event_index        as evt_index_register,
    l.block_date,
    l.block_month
  from (select * from event_data where action = 'register') l
  left join (select * from event_data where action = 'certify') r
    on l.object_id      = r.object_id
   and l.sender         = r.sender
   and l.epoch          = r.epoch
   and l.starting_epoch = r.starting_epoch
   and l.ending_epoch   = r.ending_epoch
),

pruned as (
  select
    sender,
    partner_name,
    epoch_register,
    ts_register,
    blob_hash,
    object_id,
    deletable,
    starting_epoch,
    ending_epoch,
    size_mb,
    extension,
    certify,
    ts_certify,
    epoch_certify,
    tx_register,
    evt_index_register,
    block_date,
    block_month
  from dataset
  {% if is_incremental() %}
    where {{ incremental_predicate('block_date') }}
  {% endif %}
)

-- 5) Final projection
select
  sender,
  partner_name,
  epoch_register,
  ts_register,
  blob_hash,
  object_id,
  deletable as is_deletable,
  starting_epoch,
  ending_epoch,
  size_mb,
  extension,
  certify,
  ts_certify,
  epoch_certify,
  tx_register,
  evt_index_register,
  block_date,
  block_month
from pruned