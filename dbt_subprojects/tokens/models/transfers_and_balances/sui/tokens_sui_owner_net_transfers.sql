{{
  config(
    schema = 'tokens_sui',
    alias = 'owner_net_transfers',
    materialized = 'incremental',
    file_format = 'delta',
    partition_by = ['block_date'],
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    merge_skip_unchanged = true,
    tags = ['prod_exclude'],
  )
}}

{% set sui_transfer_start_date = '2026-01-01' %} -- just ci test

with

-- load incremental object-event features used to reconstruct owner deltas
transfer_event_features as (
  select
    f.object_id,
    f.version,
    f.tx_digest,
    f.timestamp_ms,
    f.block_date,
    f.block_month,
    f.checkpoint,
    f.owner_type,
    f.receiver,
    f.coin_type,
    f.object_status,
    f.coin_balance,
    f.prev_owner,
    f.prev_balance,
    f.balance_delta,
    f.has_ownership_change
  from {{ ref('tokens_sui_object_event_deltas') }} f
  where f.block_date >= date '{{ sui_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('f.block_date') }}
    {% endif %}
),

-- load direct cross-address transfers for the same incremental window
direct_transfers as (
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
    d.coin_balance,
    d.prev_owner,
    d.prev_balance,
    d.balance_delta,
    d.has_ownership_change,
    d.tx_sender,
    d.owner_net_type,
    d.transfer_from,
    d.transfer_to,
    d.amount_raw
  from {{ ref('tokens_sui_direct_transfers') }} d
  where d.block_date >= date '{{ sui_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('d.block_date') }}
    {% endif %}
),

-- expand object events into signed per-owner deltas
owner_true_deltas as (
  select
    f.tx_digest,
    f.coin_type,
    f.receiver as owner,
    cast(f.coin_balance as decimal(38, 0)) as owner_delta_raw
  from transfer_event_features f
  where f.object_status = 'Created'
    and f.receiver is not null
    and f.coin_balance is not null
  union all
  select
    f.tx_digest,
    f.coin_type,
    f.prev_owner as owner,
    cast(-f.prev_balance as decimal(38, 0)) as owner_delta_raw
  from transfer_event_features f
  where f.object_status = 'Deleted'
    and f.prev_owner is not null
    and f.prev_balance is not null
  union all
  select
    f.tx_digest,
    f.coin_type,
    f.prev_owner as owner,
    cast(-f.prev_balance as decimal(38, 0)) as owner_delta_raw
  from transfer_event_features f
  where f.object_status = 'Mutated'
    and f.has_ownership_change
    and f.prev_owner is not null
    and f.prev_balance is not null
  union all
  select
    f.tx_digest,
    f.coin_type,
    f.receiver as owner,
    cast(f.coin_balance as decimal(38, 0)) as owner_delta_raw
  from transfer_event_features f
  where f.object_status = 'Mutated'
    and f.has_ownership_change
    and f.receiver is not null
    and f.coin_balance is not null
  union all
  select
    f.tx_digest,
    f.coin_type,
    f.receiver as owner,
    cast(f.balance_delta as decimal(38, 0)) as owner_delta_raw
  from transfer_event_features f
  where f.object_status = 'Mutated'
    and not f.has_ownership_change
    and f.receiver is not null
    and f.balance_delta != 0
),

-- aggregate expected owner net delta per tx, coin and owner
owner_true_net as (
  select
    r.tx_digest,
    r.coin_type,
    r.owner,
    sum(r.owner_delta_raw) as owner_net_delta_raw
  from owner_true_deltas r
  where r.owner is not null
  group by 1, 2, 3
),

-- expand direct transfers into signed per-owner deltas
owner_direct_deltas as (
  select
    d.tx_digest,
    d.coin_type,
    d.transfer_from as owner,
    cast(-d.amount_raw as decimal(38, 0)) as owner_delta_raw
  from direct_transfers d
  where d.transfer_from is not null
    and d.amount_raw != 0
  union all
  select
    d.tx_digest,
    d.coin_type,
    d.transfer_to as owner,
    cast(d.amount_raw as decimal(38, 0)) as owner_delta_raw
  from direct_transfers d
  where d.transfer_to is not null
    and d.amount_raw != 0
),

-- aggregate direct-transfer owner net delta per tx, coin and owner
owner_direct_net as (
  select
    r.tx_digest,
    r.coin_type,
    r.owner,
    sum(r.owner_delta_raw) as owner_net_delta_raw
  from owner_direct_deltas r
  where r.owner is not null
  group by 1, 2, 3
),

-- capture tx+coin context fields for residual reconciliation from object events
tx_coin_context_base as (
  select
    f.tx_digest,
    f.coin_type,
    max_by(f.block_month, f.timestamp_ms) as block_month,
    max_by(f.block_date, f.timestamp_ms) as block_date,
    max(f.timestamp_ms) as timestamp_ms,
    max_by(f.checkpoint, f.timestamp_ms) as checkpoint
  from transfer_event_features f
  where f.tx_digest is not null
  group by 1, 2
),

-- carry tx sender where direct transfer context is available
tx_coin_context_sender as (
  select
    d.tx_digest,
    d.coin_type,
    max(d.tx_sender) as tx_sender
  from direct_transfers d
  where d.tx_digest is not null
  group by 1, 2
),

-- merge object-event tx context with available sender enrichment
tx_coin_context as (
  select
    c.tx_digest,
    c.coin_type,
    c.block_month,
    c.block_date,
    c.timestamp_ms,
    c.checkpoint,
    s.tx_sender
  from tx_coin_context_base c
  left join tx_coin_context_sender s
    on c.tx_digest = s.tx_digest
    and c.coin_type = s.coin_type
),

-- keep only tx+coin pairs that have context in the active incremental window
owner_residual_net as (
  select
    coalesce(t.tx_digest, d.tx_digest) as tx_digest,
    coalesce(t.coin_type, d.coin_type) as coin_type,
    coalesce(t.owner, d.owner) as owner,
    coalesce(t.owner_net_delta_raw, cast(0 as decimal(38, 0)))
      - coalesce(d.owner_net_delta_raw, cast(0 as decimal(38, 0))) as residual_delta_raw
  from owner_true_net t
  full outer join owner_direct_net d
    on t.tx_digest = d.tx_digest
    and t.coin_type = d.coin_type
    and t.owner = d.owner
  inner join tx_coin_context c
    on coalesce(t.tx_digest, d.tx_digest) = c.tx_digest
    and coalesce(t.coin_type, d.coin_type) = c.coin_type
  where coalesce(t.owner, d.owner) is not null
),

-- materialize residual transfers that reconcile owner-level nets
owner_residual_transfers as (
  select
    cast(null as varbinary) as object_id,
    cast(null as decimal(20, 0)) as version,
    r.tx_digest,
    c.timestamp_ms,
    c.block_date,
    c.block_month,
    c.checkpoint,
    cast(null as varchar) as owner_type,
    case
      when r.residual_delta_raw > 0 then r.owner
      else cast(null as varbinary)
    end as receiver,
    r.coin_type,
    cast('Residual' as varchar) as object_status,
    cast(null as decimal(38, 0)) as coin_balance,
    case
      when r.residual_delta_raw < 0 then r.owner
      else cast(null as varbinary)
    end as prev_owner,
    cast(null as decimal(38, 0)) as prev_balance,
    r.residual_delta_raw as balance_delta,
    cast(false as boolean) as has_ownership_change,
    c.tx_sender,
    case
      when r.residual_delta_raw < 0 then cast('owner_residual_debit' as varchar)
      else cast('owner_residual_credit' as varchar)
    end as owner_net_type,
    case
      when r.residual_delta_raw <= 0 then r.owner
      else cast(null as varbinary)
    end as transfer_from,
    case
      when r.residual_delta_raw > 0 then r.owner
      else cast(null as varbinary)
    end as transfer_to,
    abs(r.residual_delta_raw) as amount_raw
  from owner_residual_net r
  inner join tx_coin_context c
    on r.tx_digest = c.tx_digest
    and r.coin_type = c.coin_type
  -- on full refresh, drop zero residual values because there is no stale state to neutralize
  {% if not is_incremental() %}
  where r.residual_delta_raw != 0
  {% endif %}
),

-- assemble final owner-net output and normalized key components
owner_net_transfers as (
  select
    t.*,
    case
      when t.owner_net_type in ('owner_residual_debit', 'owner_residual_credit')
      then cast('owner_residual' as varchar)
      else t.owner_net_type
    end as owner_net_type_normalized,
    case
      when t.owner_net_type in ('owner_residual_debit', 'owner_residual_credit')
      then coalesce(t.transfer_from, t.transfer_to)
      else t.transfer_from
    end as transfer_from_normalized,
    case
      when t.owner_net_type in ('owner_residual_debit', 'owner_residual_credit')
      then cast(null as varbinary)
      else t.transfer_to
    end as transfer_to_normalized
  from (
    select * from direct_transfers
    union all
    select * from owner_residual_transfers
  ) t
)

select
  {{ dbt_utils.generate_surrogate_key([
    'f.tx_digest',
    'f.coin_type',
    'f.owner_net_type_normalized',
    'f.transfer_from_normalized',
    'f.transfer_to_normalized',
    'f.object_id',
    'f.version'
  ]) }} as unique_key,
  f.object_id,
  f.version,
  f.tx_digest,
  f.timestamp_ms,
  f.block_date,
  f.block_month,
  f.checkpoint,
  f.owner_type,
  f.receiver,
  f.coin_type,
  f.object_status,
  f.coin_balance,
  f.prev_owner,
  f.prev_balance,
  f.balance_delta,
  f.has_ownership_change,
  f.tx_sender,
  f.owner_net_type,
  f.transfer_from,
  f.transfer_to,
  f.amount_raw,
  current_timestamp as _updated_at
from owner_net_transfers f
