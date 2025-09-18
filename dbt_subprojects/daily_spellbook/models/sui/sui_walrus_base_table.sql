{{ config(
  schema = 'sui_walrus',
  alias  = 'base_table',
  materialized = 'incremental',
  file_format = 'delta',
  partition_by = ['block_month'],
  incremental_strategy = 'merge',
  unique_key = ['tx_register','evt_index_register'],
  incremental_predicates = [ incremental_predicate('DBT_INTERNAL_DEST.block_date') ],
  tags=['walrus']
) }}

{% set walrus_start_date = var('walrus_start_date', '2025-09-13') %}

-- 1) Source events (keep source types; add normalized display columns)
with events as (
  select
      transaction_digest
    , event_index
    , checkpoint
    , epoch                                                    as sui_epoch
    , timestamp_ms
    , sender                                                  as sender_bin
    , package
    , module
    , event_type
    , event_json
    , ('0x' || lower(to_hex(sender)))                         as sender_hex

    -- Walrus payload fields (Trino JSON functions)
    , case 
        when json_extract_scalar(event_json, '$.blob_id') is null then null
        when starts_with(lower(json_extract_scalar(event_json, '$.blob_id')), '0x')
          then lower(json_extract_scalar(event_json, '$.blob_id'))
        else concat('0x', lower(json_extract_scalar(event_json, '$.blob_id')))
      end                                                     as blob_id_hex
    , lower(json_extract_scalar(event_json, '$.object_id'))   as object_id_hex
    , cast(json_extract_scalar(event_json, '$.start_epoch') as bigint)  as start_epoch
    , cast(json_extract_scalar(event_json, '$.end_epoch')   as bigint)  as end_epoch
    , cast(json_extract_scalar(event_json, '$.epoch')       as bigint)  as walrus_event_epoch
    , case when event_type like '%::BlobRegistered'
           then cast(json_extract_scalar(event_json, '$.size') as decimal(38,0))
           else null
      end                                                     as size_bytes
    , case when event_type like '%::BlobCertified'
           then try_cast(json_extract_scalar(event_json, '$.is_extension') as boolean)
           else null
      end                                                     as is_extension

  from {{ source('sui','events') }}
  where event_type in (
        '0xfdc88f7d7cf30afab2f82e8380d11ee8f70efb90e863d1de8616fae1bb09ea77::events::BlobRegistered',
        '0xfdc88f7d7cf30afab2f82e8380d11ee8f70efb90e863d1de8616fae1bb09ea77::events::BlobCertified'
  )
    and from_unixtime(timestamp_ms/1000) >= timestamp '{{ walrus_start_date }}'
  {% if is_incremental() %}
    and {{ incremental_predicate('from_unixtime(timestamp_ms/1000)') }}
  {% endif %}
)

-- 2) Tag action and keep both epochs (walrus + sui); keep datatypes as-is
, event_data as (
  select
      transaction_digest
    , event_index
    , timestamp_ms
    , sender_bin
    , sender_hex
    , object_id_hex
    , sui_epoch
    , walrus_event_epoch
    , start_epoch
    , end_epoch
    , blob_id_hex              as blob_id      -- use hex string consistently
    , size_bytes
    , is_extension
    , case when event_type like '%::BlobRegistered' then 'register' else 'certify' end as action
    , from_unixtime(timestamp_ms/1000)                         as block_time
    , date(from_unixtime(timestamp_ms/1000))                   as block_date
    , date_trunc('month', from_unixtime(timestamp_ms/1000))    as block_month
  from events
)
-- 3a) split and dedupe registers
, regs as (
  select *
         , row_number() over (
            partition by sender_bin, object_id_hex, start_epoch, end_epoch
            order by timestamp_ms
          ) as rn_reg
  from event_data
  where action = 'register'
)
, regs_dedup as (
  select * from regs where rn_reg = 1
)

-- 3b) match first certification at/after register
, reg_cert as (
  select
      r.sender_hex                    as sender
    , r.sender_bin
    , r.object_id_hex                 as object_id
    , r.walrus_event_epoch            as epoch_register
    , r.sui_epoch                     as sui_epoch_register
    , r.start_epoch                   as starting_epoch
    , r.end_epoch                     as ending_epoch
    , r.blob_id                       as blob_id         -- hex string
    , r.size_bytes
    , r.block_time                    as ts_register
    , r.block_date
    , r.block_month
    , r.transaction_digest            as tx_register
    , r.event_index                   as evt_index_register
    , c.block_time                    as ts_certify
    , c.walrus_event_epoch            as epoch_certify
    , c.sui_epoch                     as sui_epoch_certify
    , c.is_extension
    , row_number() over (
        partition by r.sender_bin, r.object_id_hex, r.start_epoch, r.end_epoch, r.timestamp_ms
        order by c.timestamp_ms
      ) as rn_cert
  from regs_dedup r
  left join event_data c
    on  c.action        = 'certify'
    and c.sender_bin    = r.sender_bin
    and c.object_id_hex = r.object_id_hex
    and c.start_epoch   is not distinct from r.start_epoch
    and c.end_epoch     is not distinct from r.end_epoch
    and c.timestamp_ms >= r.timestamp_ms
)

, joined as (
  select
      sender
    , epoch_register
    , ts_register
    , blob_id                         as blob_hash       -- keep hex form
    , object_id
    , cast(null as boolean)           as is_deletable
    , starting_epoch
    , ending_epoch
    , case when size_bytes is not null
           then cast(size_bytes as decimal(38,6)) / 1e6
           else null end               as size_mb
    , ts_certify
    , epoch_certify
    , tx_register
    , evt_index_register
    , block_date
    , block_month
    , is_extension
  from reg_cert
  where rn_cert = 1 or ts_certify is null
)

select * from joined
{% if is_incremental() %}
where {{ incremental_predicate('block_date') }}
{% endif %}