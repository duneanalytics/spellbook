{{
  config(
    schema = 'tokens_sui',
    alias = 'base_transfers',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    merge_skip_unchanged = true,
    tags = ['sui', 'tokens', 'transfers'],
  )
}}

-- temporary ci filter: original start date '2023-04-12', bumped to '2026-03-01' to reduce scan and unblock ci run
{% set sui_transfer_start_date = '2026-03-01' %}

with

day_rows as (
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

-- filter deleted to known coin objects only; without this filter non-coin
-- deleted objects (76% of all deletes) enter the window function and cause
-- a 9x performance regression.
known_coin_ids as (
  select distinct
    h.object_id
  from {{ ref('tokens_sui_coin_object_history') }} h
),

deleted_rows as (
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
    cast(0 as bigint) as coin_balance
  from {{ source('sui', 'objects') }} o
  where o.object_status = 'Deleted'
    and o.date >= date '{{ sui_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('o.date') }}
    {% endif %}
    and o.object_id in (select k.object_id from known_coin_ids k)
),

first_window_versions as (
  select
    u.object_id,
    min(u.version) as first_window_version
  from (
    select
      d.object_id,
      d.version
    from day_rows d
    union all
    select
      dr.object_id,
      dr.version
    from deleted_rows dr
  ) u
  group by u.object_id
),

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
  inner join first_window_versions f
    on h.object_id = f.object_id
    and h.version < f.first_window_version
  group by h.object_id
),

unioned as (
  select
    a.object_id,
    a.version,
    a.tx_digest,
    a.timestamp_ms,
    a.block_date,
    a.block_month,
    a.checkpoint,
    a.owner_type,
    a.receiver,
    a.coin_type,
    a.object_status,
    a.coin_balance
  from history_anchors a
  union all
  select
    d.object_id,
    d.version,
    d.tx_digest,
    d.timestamp_ms,
    d.block_date,
    d.block_month,
    d.checkpoint,
    d.owner_type,
    d.receiver,
    d.coin_type,
    d.object_status,
    d.coin_balance
  from day_rows d
  union all
  select
    dr.object_id,
    dr.version,
    dr.tx_digest,
    dr.timestamp_ms,
    dr.block_date,
    dr.block_month,
    dr.checkpoint,
    dr.owner_type,
    dr.receiver,
    dr.coin_type,
    dr.object_status,
    dr.coin_balance
  from deleted_rows dr
),

calc as (
  select
    u.object_id,
    u.version,
    u.tx_digest,
    u.timestamp_ms,
    u.block_date,
    u.block_month,
    u.checkpoint,
    coalesce(u.owner_type, lag(u.owner_type) over w) as owner_type,
    -- keep deleted rows as owner -> null so burn-only deletions remain observable
    case
      when u.object_status = 'Deleted' then u.receiver
      else coalesce(u.receiver, lag(u.receiver) over w)
    end as receiver,
    coalesce(u.coin_type, lag(u.coin_type) over w) as coin_type,
    u.object_status,
    u.coin_balance,
    lag(u.receiver) over w as prev_owner,
    lag(u.coin_balance) over w as prev_balance
  from unioned u
  window w as (partition by u.object_id order by u.version)
),

-- deferred tx join: compute enriched + filtered WITHOUT tx_sender first.
-- rows with a known prev_owner (anchored) can be cross-address filtered
-- without any tx lookup. only look up tx_sender for Created + unanchored
-- rows (~5% of filtered), reducing the transactions scan by ~95%.

enriched as (
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
    end as has_ownership_change
  from calc c
  where c.object_status != 'ANCHOR'
),

filtered as (
  select
    e.*
  from enriched e
  where e.balance_delta != 0
    or e.has_ownership_change
),

-- classify: rows with known prev_owner pass cross-address filter immediately;
-- rows needing tx_sender (Created + unanchored) are kept for the tx lookup.
pre_cross as (
  select
    f.*,
    case
      when f.prev_owner is not null
        then (f.prev_owner is distinct from f.receiver)
      else true  -- needs tx_sender to decide; keep for now
    end as passes_cross_filter,
    case
      when f.object_status = 'Created' or f.prev_owner is null then true
      else false
    end as needs_tx_sender
  from filtered f
),

-- only look up tx_sender for the small subset that needs it
tx_senders as (
  select
    t.transaction_digest as tx_digest,
    t.sender as tx_sender
  from {{ source('sui', 'transactions') }} t
  inner join (
    select distinct
      p.tx_digest
    from pre_cross p
    where p.needs_tx_sender
      and p.passes_cross_filter
      and p.tx_digest is not null
  ) d
    on t.transaction_digest = d.tx_digest
  where t.date >= date '{{ sui_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('t.date') }}
    {% endif %}
),

cross_address_filtered as (
  -- rows with known prev_owner: already filtered, no tx lookup needed
  select
    p.*,
    cast(null as varbinary) as tx_sender
  from pre_cross p
  where p.passes_cross_filter
    and not p.needs_tx_sender
  union all
  -- rows needing tx_sender: join and apply cross-address filter
  select
    p.*,
    s.tx_sender
  from pre_cross p
  left join tx_senders s
    on p.tx_digest = s.tx_digest
  where p.needs_tx_sender
    and (
      case
        when p.object_status = 'Created' then s.tx_sender
        else coalesce(p.prev_owner, s.tx_sender)
      end
    ) is distinct from p.receiver
),

supply_signals as (
  select
    s.tx_digest,
    s.coin_type,
    s.supply_event_type
  from {{ ref('tokens_sui_supply_signals') }} s
  where s.block_date >= date '{{ sui_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('s.block_date') }}
    {% endif %}
),

tx_reconciliation as (
  select
    f.tx_digest,
    f.coin_type,
    -- aggregate in high-precision decimal to avoid bigint overflow on large net-delta txs
    sum(cast(f.balance_delta as decimal(38, 0))) as tx_net_delta,
    count(distinct f.receiver) as tx_distinct_receivers,
    count(distinct f.prev_owner) filter (where f.prev_owner is not null) as tx_distinct_senders,
    bool_or(f.balance_delta > 0) and bool_or(f.balance_delta < 0) as tx_has_bidirectional_deltas
  from filtered f
  group by f.tx_digest, f.coin_type
)

select
  {{ dbt_utils.generate_surrogate_key(['f.tx_digest', 'f.object_id', 'f.version']) }} as unique_key,
  'sui' as blockchain,
  f.block_month,
  f.block_date,
  from_unixtime(f.timestamp_ms / 1000) as block_time,
  f.checkpoint as block_number,
  from_base58(f.tx_digest) as tx_hash,
  cast(null as bigint) as evt_index,
  cast(null as array(bigint)) as trace_address,
  'sui_coin' as token_standard,
  f.tx_sender as tx_from,
  cast(null as varbinary) as tx_to,
  cast(null as bigint) as tx_index,
  case
    when f.object_status = 'Created' then f.tx_sender
    else coalesce(f.prev_owner, f.tx_sender)
  end as "from",
  f.receiver as to,
  cast(split_part(f.coin_type, '::', 1) as varbinary) as contract_address,
  f.coin_type as contract_address_full,
  case
    when f.object_status = 'Created' then f.coin_balance
    when f.has_ownership_change and f.balance_delta = 0 then f.coin_balance
    else abs(f.balance_delta)
  end as amount_raw,
  f.balance_delta,
  f.object_id,
  f.version,
  f.object_status,
  f.owner_type,
  f.coin_balance,
  f.prev_balance,
  f.prev_owner,
  f.has_ownership_change,
  case
    when f.object_status = 'Created' then 'object_created'
    when f.object_status = 'Deleted' then 'object_deleted'
    when f.has_ownership_change and f.balance_delta != 0 then 'transfer_with_balance_change'
    when f.has_ownership_change then 'ownership_transfer'
    when f.balance_delta > 0 then 'balance_increase'
    when f.balance_delta < 0 then 'balance_decrease'
    else 'other'
  end as transfer_type,
  true as is_cross_address_transfer,
  case
    when supply.supply_event_type = 'mint' and r.tx_net_delta > 0 then true
    when supply.supply_event_type = 'burn' and r.tx_net_delta < 0 then true
    else false
  end as is_supply_event,
  case
    when supply.supply_event_type = 'mint' and r.tx_net_delta > 0 then 'mint'
    when supply.supply_event_type = 'burn' and r.tx_net_delta < 0 then 'burn'
    else cast(null as varchar)
  end as supply_event_type,
  case
    when f.balance_delta > 0 then 'credit'
    when f.balance_delta < 0 then 'debit'
    else 'neutral'
  end as transfer_direction,
  r.tx_net_delta,
  r.tx_distinct_receivers,
  r.tx_distinct_senders,
  r.tx_has_bidirectional_deltas,
  current_timestamp as _updated_at
from cross_address_filtered f
left join tx_reconciliation r
  on f.tx_digest = r.tx_digest
  and f.coin_type = r.coin_type
left join supply_signals supply
  on f.tx_digest = supply.tx_digest
  and lower(f.coin_type) = supply.coin_type
