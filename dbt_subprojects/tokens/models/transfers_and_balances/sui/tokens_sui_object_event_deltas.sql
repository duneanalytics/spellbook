-- depends_on: {{ ref('tokens_sui_coin_object_latest_state') }}

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

{% set sui_transfer_start_date = '2026-01-01' %} -- ci test, revert to '2023-04-12'

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

anchor_object_ids as (
  select object_id from coin_window_ids
  union
  select object_id from deleted_objects_raw
),

-- first window version per object; clips helper rows to strictly prior states
-- so helper entries within the current window are never reused as anchors
-- (also yields zero anchors on full refresh, matching prior behavior).
window_first_version as (
  select
    u.object_id,
    min(u.version) as min_window_version
  from (
    select object_id, version from coin_object_history
    union all
    select object_id, version from deleted_objects_raw
  ) u
  group by 1
),

-- load latest prior object state per object as anchor context
history_anchors as (
  select
    l.object_id,
    l.version,
    cast(null as varchar) as tx_digest,
    l.timestamp_ms,
    l.block_date,
    l.block_month,
    l.checkpoint,
    l.owner_type,
    l.receiver,
    l.coin_type,
    cast('ANCHOR' as varchar) as object_status,
    l.coin_balance
  from {{ ref('tokens_sui_coin_object_latest_state') }} l
  inner join window_first_version w
    on l.object_id = w.object_id
  where l.version < w.min_window_version
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
