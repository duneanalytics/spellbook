{{ config(
  schema = 'sui_walrus',
  alias = 'base_table',
  materialized = 'incremental',
  file_format = 'delta',
  partition_by = ['block_month'],
  incremental_strategy = 'merge',
  unique_key = ['tx_register','evt_index_register'],
  incremental_predicates = [ incremental_predicate('DBT_INTERNAL_DEST.block_date') ],
  tags=['walrus']
) }}

{% set walrus_start_date = var('walrus_start_date', '2025-09-13') %}

-- 1) Walrus events
with events as (
  select
      ('0x' || lower(to_hex(sender)))                         as sender            -- varbinary → 0x-hex
    , epoch                                                   as sui_epoch
    , cast(json_extract_scalar(event_json, '$.epoch') as bigint)     as epoch
    , event_type
    , event_json
    , ('0x' || lower(to_hex(from_base58(transaction_digest)))) as transaction_digest_hex
    , event_index
    , timestamp_ms
    , from_unixtime(timestamp_ms/1000)                        as block_time
    , date(from_unixtime(timestamp_ms/1000))                  as block_date
    , date_trunc('month', from_unixtime(timestamp_ms/1000))   as block_month
  from {{ source('sui','events') }}
  where event_type in (
    '0xfdc88f7d7cf30afab2f82e8380d11ee8f70efb90e863d1de8616fae1bb09ea77::events::BlobRegistered',
    '0xfdc88f7d7cf30afab2f82e8380d11ee8f70efb90e863d1de8616fae1bb09ea77::events::BlobCertified'
  )
    and from_unixtime(timestamp_ms/1000) >= timestamp '{{ walrus_start_date }}'
  {% if is_incremental() %}
    and {{ incremental_predicate('from_unixtime(timestamp_ms/1000)') }}
  {% endif %}
),

-- 2) Normalize payloads
event_data as (
  select
      transaction_digest_hex
    , event_index
    , timestamp_ms
    , sender
    , sui_epoch
    , epoch
    , case when event_type like '%::BlobRegistered' then 'register' else 'certify' end as action
    , case
        when json_extract_scalar(event_json, '$.blob_id') is null then null
        when starts_with(lower(json_extract_scalar(event_json, '$.blob_id')), '0x')
          then lower(json_extract_scalar(event_json, '$.blob_id'))
        else concat('0x', lower(json_extract_scalar(event_json, '$.blob_id')))
      end                                                   as blob_id
    , lower(json_extract_scalar(event_json, '$.object_id')) as object_id
    , cast(json_extract_scalar(event_json, '$.deletable') as boolean) as deletable
    , cast(json_extract_scalar(event_json, '$.end_epoch') as bigint)  as ending_epoch
    , cast(null as bigint)                                            as starting_epoch
    , case
        when event_type like '%::BlobRegistered'
          then cast(json_extract_scalar(event_json, '$.size') as decimal(38,0))
        else null end                                         as size_bytes
    , from_unixtime(timestamp_ms/1000)                        as block_time
    , date(from_unixtime(timestamp_ms/1000))                  as block_date
    , date_trunc('month', from_unixtime(timestamp_ms/1000))   as block_month
  from events
),

-- 3) Join register ↔ certify on strict keys
joined as (
  select
      l.sender
    , l.epoch                                as epoch_register
    , l.block_time                            as ts_register
    , l.blob_id                               as blob_hash
    , l.object_id
    , l.deletable                             as is_deletable
    , l.starting_epoch
    , l.ending_epoch
    , case when l.size_bytes is not null
           then cast(l.size_bytes as decimal(38,6)) / cast(1000000 as decimal(38,6))
           else null end                      as size_mb
    , r.block_time                            as ts_certify
    , r.epoch                                 as epoch_certify
    , l.transaction_digest_hex                as tx_register
    , l.event_index                           as evt_index_register
    , l.block_date
    , l.block_month
  from event_data l
  left join event_data r
    on r.action = 'certify'
   and l.action = 'register'
   and l.object_id      = r.object_id
   and l.sender         = r.sender
   and l.epoch          = r.epoch
   and l.starting_epoch IS NOT DISTINCT FROM r.starting_epoch
   and l.ending_epoch   IS NOT DISTINCT FROM r.ending_epoch
   and r.timestamp_ms  >= l.timestamp_ms
)

-- 4) Final (only required business columns + tech cols for incremental)
select
    sender
  , epoch_register
  , ts_register
  , blob_hash
  , object_id
  , is_deletable
  , starting_epoch
  , ending_epoch
  , size_mb
  , ts_certify
  , epoch_certify

  -- internal columns used for MERGE & partitioning; not for end users
  , tx_register
  , evt_index_register
  , block_date
  , block_month
from joined
{% if is_incremental() %}
where {{ incremental_predicate('joined.block_date') }}
{% endif %}