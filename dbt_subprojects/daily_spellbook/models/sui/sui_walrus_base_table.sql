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
      ('0x' || lower(to_hex(sender)))                          as sender            -- varbinary → 0x-hex
    , epoch                                                    as sui_epoch
    , cast(json_extract_scalar(event_json, '$.epoch') as bigint) as walrus_epoch
    , event_type
    , event_json
    , ('0x' || lower(to_hex(from_base58(transaction_digest))))  as transaction_digest_hex
    , event_index
    , timestamp_ms
    , from_unixtime(timestamp_ms/1000)                         as block_time
    , date(from_unixtime(timestamp_ms/1000))                   as block_date
    , date_trunc('month', from_unixtime(timestamp_ms/1000))    as block_month
  from {{ source('sui','events') }}
  where event_type in (
        '0xfdc88f7d7cf30afab2f82e8380d11ee8f70efb90e863d1de8616fae1bb09ea77::events::BlobRegistered'
      , '0xfdc88f7d7cf30afab2f82e8380d11ee8f70efb90e863d1de8616fae1bb09ea77::events::BlobCertified'
  )
    and from_unixtime(timestamp_ms/1000) >= timestamp '{{ walrus_start_date }}'
  {% if is_incremental() %}
    and {{ incremental_predicate('from_unixtime(timestamp_ms/1000)') }}
  {% endif %}
)

-- 2) Normalize payloads (keep both epochs)
, event_data as (
  select
      transaction_digest_hex
    , event_index
    , timestamp_ms
    , sender
    , sui_epoch
    , walrus_epoch
    , case when event_type like '%::BlobRegistered' then 'register' else 'certify' end as action
    , case
        when json_extract_scalar(event_json, '$.blob_id') is null then null
        when starts_with(lower(json_extract_scalar(event_json, '$.blob_id')), '0x')
          then lower(json_extract_scalar(event_json, '$.blob_id'))
        else concat('0x', lower(json_extract_scalar(event_json, '$.blob_id')))
      end                                                    as blob_id
    , lower(json_extract_scalar(event_json, '$.object_id'))  as object_id
    , cast(json_extract_scalar(event_json, '$.deletable') as boolean) as deletable
    , cast(json_extract_scalar(event_json, '$.start_epoch') as bigint) as starting_epoch
    , cast(json_extract_scalar(event_json, '$.end_epoch')   as bigint) as ending_epoch
    , case
        when event_type like '%::BlobRegistered'
          then cast(json_extract_scalar(event_json, '$.size') as decimal(38,0))
        else null end                                          as size_bytes
    , from_unixtime(timestamp_ms/1000)                         as block_time
    , date(from_unixtime(timestamp_ms/1000))                   as block_date
    , date_trunc('month', from_unixtime(timestamp_ms/1000))    as block_month
  from events
)

-- 3a) split registers / certs
, regs as (
  select
      *
  from event_data
  where action = 'register'
)
, certs as (
  select
      *
  from event_data
  where action = 'certify'
)

-- 3b) dedupe register retries (keep earliest)
, regs_ranked as (
  select
      r.*
    , row_number() over (
        partition by r.sender, r.blob_id, r.walrus_epoch, r.object_id
        order by r.timestamp_ms
      ) as rn_reg
  from regs r
)
, regs_dedup as (
  select
      *
  from regs_ranked
  where rn_reg = 1
)

-- 3c) match the FIRST certification at/after the register (no strict epoch equality)
, reg_cert_all as (
  select
      r.sender
    , r.blob_id
    , r.object_id
    , r.walrus_epoch                                        as walrus_epoch_register
    , r.sui_epoch                                            as sui_epoch_register
    , r.starting_epoch
    , r.ending_epoch
    , r.size_bytes
    , r.block_time                                           as ts_register
    , r.block_date
    , r.block_month
    , r.transaction_digest_hex                               as tx_register
    , r.event_index                                          as evt_index_register
    , c.block_time                                           as ts_certify
    , c.walrus_epoch                                         as walrus_epoch_certify
    , c.sui_epoch                                            as sui_epoch_certify
    , c.timestamp_ms                                         as c_ts_ms
    , row_number() over (
        partition by r.sender, r.blob_id, r.object_id, r.walrus_epoch, r.starting_epoch, r.ending_epoch, r.transaction_digest_hex, r.event_index
        order by c.timestamp_ms
      )                                                      as rn_cert
  from regs_dedup r
  left join certs c
    on c.sender      = r.sender
   and c.object_id   = r.object_id
   and c.blob_id     = r.blob_id
   and c.starting_epoch is not distinct from r.starting_epoch
   and c.ending_epoch   is not distinct from r.ending_epoch
   and c.timestamp_ms  >= r.timestamp_ms
)

, joined as (
  select
      sender
    , walrus_epoch_register                                   as epoch_register
    , ts_register
    , blob_id                                                 as blob_hash
    , object_id
    , cast(json_extract_scalar('{{ "{}" }}', '$.x') as boolean) as is_deletable  -- placeholder removed; see note*
    , starting_epoch
    , ending_epoch
    , case when size_bytes is not null
           then cast(size_bytes as decimal(38,6)) / cast(1000000 as decimal(38,6))
           else null end                                       as size_mb
    , ts_certify
    , walrus_epoch_certify                                    as epoch_certify
    , tx_register
    , evt_index_register
    , block_date
    , block_month
  from reg_cert_all
  where rn_cert = 1 or ts_certify is null
)

select
    sender
  , epoch_register
  , ts_register
  , blob_hash
  , object_id
  , nullif(is_deletable, false) as is_deletable  -- optional: if you capture deletable only on register, restore your original col as needed
  , starting_epoch
  , ending_epoch
  , size_mb
  , ts_certify
  , epoch_certify
  , tx_register
  , evt_index_register
  , block_date
  , block_month
from joined
{% if is_incremental() %}
where {{ incremental_predicate('joined.block_date') }}
{% endif %}