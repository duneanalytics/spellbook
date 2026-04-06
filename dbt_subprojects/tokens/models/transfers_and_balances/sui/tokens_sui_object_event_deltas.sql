{{
  config(
    schema = 'tokens_sui',
    alias = 'object_event_deltas',
    materialized = 'incremental',
    file_format = 'delta',
    partition_by = ['block_date'],
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'object_id', 'version'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    merge_skip_unchanged = true,
  )
}}

{% set sui_transfer_start_date = '2026-01-01' %} -- just ci test

with

-- load created and mutated coin object history for the active window
coin_object_history as (
  select
    h.object_id,
    h.version,
    h.tx_digest,
    h.timestamp_ms,
    h.block_date,
    h.block_month,
    h.checkpoint,
    h.owner_type,
    h.receiver,
    h.coin_type,
    h.object_status,
    h.coin_balance
  from {{ ref('tokens_sui_coin_object_history') }} h
  where h.block_date >= date '{{ sui_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('h.block_date') }}
    {% endif %}
),

coin_window_ids as (
  select distinct h.object_id
  from coin_object_history h
),

-- load deleted objects in the active window
deleted_objects_raw as (
  select
    o.object_id,
    o.version,
    o.previous_transaction as tx_digest,
    o.timestamp_ms,
    o.date as block_date,
    cast(date_trunc('month', o.date) as date) as block_month,
    o.checkpoint,
    cast(null as varchar) as owner_type,
    cast(null as varbinary) as receiver,
    cast(null as varchar) as coin_type,
    o.object_status,
    cast(0 as decimal(38, 0)) as coin_balance
  from {{ source('sui', 'objects') }} o
  where o.object_status = 'Deleted'
    and o.date >= date '{{ sui_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('o.date') }}
    {% endif %}
),

-- load latest prior object state per object as anchor context
history_anchors as (
  select
    h.object_id,
    max(h.version) as version,
    cast(null as varchar) as tx_digest,
    max_by(h.timestamp_ms, h.version) as timestamp_ms,
    cast(max_by(h.block_date, h.version) as date) as block_date,
    cast(date_trunc('month', max_by(h.block_date, h.version)) as date) as block_month,
    max_by(h.checkpoint, h.version) as checkpoint,
    max_by(h.owner_type, h.version) as owner_type,
    max_by(h.receiver, h.version) as receiver,
    max_by(h.coin_type, h.version) as coin_type,
    cast('ANCHOR' as varchar) as object_status,
    max_by(h.coin_balance, h.version) as coin_balance
  from {{ ref('tokens_sui_coin_object_history') }} h
  inner join (
    select object_id from coin_window_ids
    union all
    select object_id from deleted_objects_raw
  ) f on h.object_id = f.object_id
  where 1 = 1
    {% if is_incremental() %}
    and not {{ incremental_predicate('h.block_date') }}
    {% else %}
    and h.block_date < date '{{ sui_transfer_start_date }}'
    {% endif %}
  group by 1
),

-- prune deleted rows that cannot survive downstream lag-based filtering
deleted_objects as (
  select d.*
  from deleted_objects_raw d
  left join history_anchors a
    on d.object_id = a.object_id
  left join coin_window_ids c
    on d.object_id = c.object_id
  where a.object_id is not null
    or c.object_id is not null
),

-- combine history and anchors to compute previous owner and balance
object_state_deltas as (
  select
    u.object_id,
    u.version,
    u.tx_digest,
    u.timestamp_ms,
    u.block_date,
    u.block_month,
    u.checkpoint,
    coalesce(u.owner_type, lag(u.owner_type) over w) as owner_type,
    -- keep deleted entries as owner -> null so burn-only deletions remain observable
    case
      when u.object_status = 'Deleted' then u.receiver
      else coalesce(u.receiver, lag(u.receiver) over w)
    end as receiver,
    coalesce(u.coin_type, lag(u.coin_type) over w) as coin_type,
    u.object_status,
    u.coin_balance,
    lag(u.receiver) over w as prev_owner,
    lag(u.coin_balance) over w as prev_balance
  from (
    select * from history_anchors
    union all
    select * from coin_object_history
    union all
    select * from deleted_objects
  ) u
  window w as (partition by u.object_id order by u.version)
)

select
  c.object_id,
  c.version,
  c.tx_digest,
  c.timestamp_ms,
  c.block_date,
  c.block_month,
  c.checkpoint,
  c.owner_type,
  c.receiver,
  c.coin_type,
  c.object_status,
  c.coin_balance,
  c.prev_owner,
  c.prev_balance,
  c.coin_balance - coalesce(c.prev_balance, 0) as balance_delta,
  case
    when c.object_status in ('Created', 'Deleted') then false
    when c.object_status = 'Mutated'
      and c.prev_owner is not null
      and c.prev_owner != c.receiver then true
    else false
  end as has_ownership_change,
  current_timestamp as _updated_at
from object_state_deltas c
where c.object_status != 'ANCHOR'
  and (c.coin_type is not null or c.prev_balance is not null)
