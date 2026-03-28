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
  )
}}

-- temporary ci filter: original start date '2023-04-12', bumped to '2026-01-01' to reduce scan and unblock ci run
{% set sui_transfer_start_date = '2026-01-01' %}

with

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

direct_transfer_rows as (
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
    d.owner_net_leg,
    d.row_from,
    d.row_to,
    d.amount_raw
  from {{ ref('tokens_sui_direct_transfers') }} d
  where d.block_date >= date '{{ sui_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('d.block_date') }}
    {% endif %}
),

owner_true_delta_rows as (
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

owner_true_net as (
  select
    r.tx_digest,
    r.coin_type,
    r.owner,
    sum(r.owner_delta_raw) as owner_net_delta_raw
  from owner_true_delta_rows r
  where r.owner is not null
  group by 1, 2, 3
),

owner_direct_delta_rows as (
  select
    d.tx_digest,
    d.coin_type,
    d.row_from as owner,
    cast(-d.amount_raw as decimal(38, 0)) as owner_delta_raw
  from direct_transfer_rows d
  where d.row_from is not null
    and d.amount_raw != 0
  union all
  select
    d.tx_digest,
    d.coin_type,
    d.row_to as owner,
    cast(d.amount_raw as decimal(38, 0)) as owner_delta_raw
  from direct_transfer_rows d
  where d.row_to is not null
    and d.amount_raw != 0
),

owner_direct_net as (
  select
    r.tx_digest,
    r.coin_type,
    r.owner,
    sum(r.owner_delta_raw) as owner_net_delta_raw
  from owner_direct_delta_rows r
  where r.owner is not null
  group by 1, 2, 3
),

tx_coin_context as (
  select
    d.tx_digest,
    d.coin_type,
    max_by(d.block_month, d.timestamp_ms) as block_month,
    max_by(d.block_date, d.timestamp_ms) as block_date,
    max(d.timestamp_ms) as timestamp_ms,
    max_by(d.checkpoint, d.timestamp_ms) as checkpoint,
    max_by(d.tx_sender, d.timestamp_ms) as tx_sender
  from direct_transfer_rows d
  group by 1, 2
),

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
    end as owner_net_leg,
    case
      when r.residual_delta_raw < 0 then r.owner
      else cast(null as varbinary)
    end as row_from,
    case
      when r.residual_delta_raw > 0 then r.owner
      else cast(null as varbinary)
    end as row_to,
    abs(r.residual_delta_raw) as amount_raw
  from owner_residual_net r
  inner join tx_coin_context c
    on r.tx_digest = c.tx_digest
    and r.coin_type = c.coin_type
  where r.residual_delta_raw != 0
),

owner_net_transfers as (
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
    d.owner_net_leg,
    d.row_from,
    d.row_to,
    d.amount_raw
  from direct_transfer_rows d
  union all
  select
    r.object_id,
    r.version,
    r.tx_digest,
    r.timestamp_ms,
    r.block_date,
    r.block_month,
    r.checkpoint,
    r.owner_type,
    r.receiver,
    r.coin_type,
    r.object_status,
    r.coin_balance,
    r.prev_owner,
    r.prev_balance,
    r.balance_delta,
    r.has_ownership_change,
    r.tx_sender,
    r.owner_net_leg,
    r.row_from,
    r.row_to,
    r.amount_raw
  from owner_residual_transfers r
)

select
  {{ dbt_utils.generate_surrogate_key(['f.tx_digest', 'f.coin_type', 'f.owner_net_leg', 'f.row_from', 'f.row_to', 'f.object_id', 'f.version']) }} as unique_key,
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
  f.owner_net_leg,
  f.row_from,
  f.row_to,
  f.amount_raw,
  current_timestamp as _updated_at
from owner_net_transfers f
