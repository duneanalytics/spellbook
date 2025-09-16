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
      ('0x' || lower(to_hex(sender)))                 as sender
    , epoch                                           as sui_epoch
    , {{ j_bigint('event_json', '$.epoch') }}         as epoch            -- payload epoch
    , event_type
    , event_json
    , ('0x' || lower(to_hex(from_base58(transaction_digest)))) as transaction_digest
    , event_index
    , timestamp_ms
    , from_unixtime(timestamp_ms/1000)                as block_time
    , date(from_unixtime(timestamp_ms/1000))          as block_date
    , date_trunc('month', from_unixtime(timestamp_ms/1000)) as block_month
  from {{ source('sui','events') }}
  where event_type in (
    '0xfdc88f7d7cf30afab2f82e8380d11ee8f70efb90e863d1de8616fae1bb09ea77::events::BlobRegistered',
    '0xfdc88f7d7cf30afab2f82e8380d11ee8f70efb90e863d1de8616fae1bb09ea77::events::BlobCertified'
  )
    and from_unixtime(timestamp_ms/1000) >= timestamp '{{ walrus_start_date }}'
),

events_filtered as (
  select *
  from events
  {% if is_incremental() %}
    where {{ incremental_predicate('block_time') }}      -- ts-grain prune
  {% endif %}
),

-- 2) Normalize payloads (Snowflake-parity)
event_data as (
  select
      transaction_digest
    , event_index
    , timestamp_ms
    , sender
    , sui_epoch
    , epoch
    , case
        when event_type = '0xfdc88f7d7cf30afab2f82e8380d11ee8f70efb90e863d1de8616fae1bb09ea77::events::BlobRegistered'
          then 'register' else 'certify' end as action

    , case
        when {{ j_str('event_json', '$.blob_id') }} is null then null
        when starts_with({{ j_str('event_json', '$.blob_id') }}, '0x')
          then lower({{ j_str('event_json', '$.blob_id') }})
        else concat('0x', lower({{ j_str('event_json', '$.blob_id') }}))
      end as blob_id

    , lower({{ j_str('event_json', '$.object_id') }})          as object_id

    , {{ j_bool('event_json', '$.deletable') }}                 as deletable
    , {{ j_bigint('event_json', '$.epoch') }}                   as starting_epoch
    , {{ j_bigint('event_json', '$.end_epoch') }}               as ending_epoch

    , case
        when event_type = '0xfdc88f7d7cf30afab2f82e8380d11ee8f70efb90e863d1de8616fae1bb09ea77::events::BlobRegistered'
          then {{ j_num('event_json', '$.size') }}
        else null end as size

    , case
        when event_type = '0xfdc88f7d7cf30afab2f82e8380d11ee8f70efb90e863d1de8616fae1bb09ea77::events::BlobCertified'
          then {{ j_bool('event_json', '$.is_extension') }}
        else null end as is_extension

    , from_unixtime(timestamp_ms/1000)                      as block_time
    , date(from_unixtime(timestamp_ms/1000))                as block_date
    , date_trunc('month', from_unixtime(timestamp_ms/1000)) as block_month
  from events_filtered
),

-- 3) Split and join register ↔ certify
dataset as (
  select
    l.sender
  , l.epoch         as epoch_register
  , l.block_time    as ts_register
  , l.blob_id       as blob_hash
  , l.object_id
  , l.deletable
  , l.starting_epoch
  , l.ending_epoch
  , case when l.size is not null
         then cast(l.size as decimal(38,6)) / cast(1000000 as decimal(38,6))
         else null end as size_mb
  , r.is_extension
  , case when r.sender is null then 0 else 1 end as certify
  , r.block_time    as ts_certify
  , r.epoch         as epoch_certify
  , l.transaction_digest as tx_register
  , l.event_index        as evt_index_register
  , l.block_date
  , l.block_month
  from (select * from event_data where action = 'register') l
  left join (select * from event_data where action = 'certify') r
    on l.object_id      = r.object_id
   and l.sender         = r.sender
   and l.epoch          = r.epoch
   and l.starting_epoch = r.starting_epoch
   and l.ending_epoch   = r.ending_epoch
),

-- 4) Day-grain pruning aligned with MERGE predicate
pruned as (
  select
    sender
  , epoch_register
  , ts_register
  , blob_hash
  , object_id
  , deletable
  , starting_epoch
  , ending_epoch
  , size_mb
  , is_extension
  , certify
  , ts_certify
  , epoch_certify
  , tx_register
  , evt_index_register
  , block_date
  , block_month
  from dataset
  {% if is_incremental() %}
    where {{ incremental_predicate('block_date') }}
  {% endif %}
)

-- 5) Final projection
select
  sender
, epoch_register
, ts_register
, blob_hash
, object_id
, deletable as is_deletable
, starting_epoch
, ending_epoch
, size_mb
, is_extension
, certify
, ts_certify
, epoch_certify
, tx_register
, evt_index_register
, block_date
, block_month
from pruned