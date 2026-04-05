{{
  config(
    schema = 'tokens_sui',
    alias = 'supply_events',
    materialized = 'incremental',
    file_format = 'delta',
    partition_by = ['block_date'],
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    merge_skip_unchanged = true,
  )
}}

{% set sui_transfer_start_date = '2026-01-01' %} -- just ci test

with

supply_events as (
  select
    e.date as block_date,
    cast(date_trunc('month', e.date) as date) as block_month,
    from_unixtime(e.timestamp_ms / 1000) as block_time,
    e.checkpoint,
    e.transaction_digest as tx_digest,
    e.event_index,
    e.sender as tx_from,
    e.sender as "from",
    case
      when lower(e.event_type) like '%treasury::mint<%'
      then from_hex(substr(json_extract_scalar(e.event_json, '$.recipient'), 3))
      else cast(null as varbinary)
    end as to,
    regexp_replace(
      regexp_extract(lower(e.event_type), 'treasury::(?:mint|burn)<([^>]+)>', 1),
      '^0x0*([0-9a-f]+)(::.*)$',
      '0x$1$2'
    ) as coin_type,
    try_cast(json_extract_scalar(e.event_json, '$.amount') as decimal(38, 0)) as amount_raw,
    case
      when lower(e.event_type) like '%treasury::mint<%' then cast('mint' as varchar)
      when lower(e.event_type) like '%treasury::burn<%' then cast('burn' as varchar)
      else cast(null as varchar)
    end as supply_event_type
  from {{ source('sui', 'events') }} e
  where e.date >= date '{{ sui_transfer_start_date }}'
    and (
      lower(e.event_type) like '%treasury::mint<%'
      or lower(e.event_type) like '%treasury::burn<%'
    )
    {% if is_incremental() %}
    and {{ incremental_predicate('e.date') }}
    {% endif %}
)

select
  {{ dbt_utils.generate_surrogate_key([
    's.tx_digest',
    's.coin_type',
    's.supply_event_type',
    'cast(s.event_index as varchar)'
  ]) }} as unique_key,
  s.block_month,
  s.block_date,
  s.block_time,
  s.checkpoint,
  s.tx_digest,
  s.event_index,
  s.tx_from,
  s."from",
  s.to,
  s.coin_type,
  s.amount_raw,
  s.supply_event_type,
  current_timestamp as _updated_at
from supply_events s
where s.coin_type is not null
  and s.amount_raw > 0
  and s.supply_event_type is not null
