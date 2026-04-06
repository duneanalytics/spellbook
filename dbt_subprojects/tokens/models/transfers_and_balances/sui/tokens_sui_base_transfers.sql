{{
  config(
    schema = 'tokens_sui',
    alias = 'base_transfers',
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

object_transfers_source as (
  select
    o.unique_key,
    o.object_id,
    o.version,
    o.tx_digest,
    o.timestamp_ms,
    o.block_date,
    o.block_month,
    o.checkpoint,
    o.owner_type,
    o.receiver,
    o.coin_type,
    regexp_replace(
      lower(o.coin_type),
      '^0x0*([0-9a-f]+)(::.*)$',
      '0x$1$2'
    ) as coin_type_normalized,
    o.object_status,
    o.coin_balance,
    o.prev_owner,
    o.prev_balance,
    o.balance_delta,
    o.has_ownership_change,
    o.tx_sender,
    o.owner_net_type,
    o.transfer_from,
    o.transfer_to,
    o.amount_raw
  from {{ ref('tokens_sui_owner_net_transfers') }} o
  where o.block_date >= date '{{ sui_transfer_start_date }}'
    and o.amount_raw != 0
    {% if is_incremental() %}
    and {{ incremental_predicate('o.block_date') }}
    {% endif %}
),

supply_events as (
  select
    s.unique_key,
    s.block_month,
    s.block_date,
    s.block_time,
    s.checkpoint,
    s.tx_digest,
    s.tx_from,
    s."from",
    s.to,
    s.coin_type,
    regexp_replace(
      lower(s.coin_type),
      '^0x0*([0-9a-f]+)(::.*)$',
      '0x$1$2'
    ) as coin_type_normalized,
    s.amount_raw,
    case
      when s.supply_event_type = 'mint' then cast(s.amount_raw as decimal(38, 0))
      when s.supply_event_type = 'burn' then cast(-s.amount_raw as decimal(38, 0))
      else cast(null as decimal(38, 0))
    end as balance_delta,
    s.supply_event_type
  from {{ ref('tokens_sui_supply_events') }} s
  where s.block_date >= date '{{ sui_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('s.block_date') }}
    {% endif %}
),

tx_coin_reconciliation as (
  select
    f.tx_digest,
    f.coin_type,
    sum(cast(f.balance_delta as decimal(38, 0))) as tx_net_delta
  from {{ ref('tokens_sui_object_event_deltas') }} f
  where f.block_date >= date '{{ sui_transfer_start_date }}'
    and (
      f.balance_delta != 0
      or f.has_ownership_change
    )
    {% if is_incremental() %}
    and {{ incremental_predicate('f.block_date') }}
    {% endif %}
  group by 1, 2
),

object_transfers as (
  select
    o.unique_key,
    'sui' as blockchain,
    o.block_month,
    o.block_date,
    from_unixtime(o.timestamp_ms / 1000) as block_time,
    o.checkpoint,
    o.tx_digest,
    'sui_coin' as token_standard,
    o.tx_sender as tx_from,
    o.transfer_from as "from",
    o.transfer_to as to,
    coalesce(
      o.transfer_from,
      case
        when o.tx_sender is not null and o.tx_sender is distinct from o.transfer_to then o.tx_sender
        else cast(null as varbinary)
      end,
      case
        when o.prev_owner is not null and o.prev_owner is distinct from o.transfer_to then o.prev_owner
        else cast(null as varbinary)
      end,
      o.transfer_to
    ) as from_resolved,
    case
      when o.transfer_from is not null then 'observed'
      when o.tx_sender is not null and o.tx_sender is distinct from o.transfer_to then 'derived_tx_sender'
      when o.prev_owner is not null and o.prev_owner is distinct from o.transfer_to then 'derived_prev_owner'
      when o.transfer_to is not null then 'mirrored_to'
      else 'unresolved_null'
    end as from_resolution_type,
    coalesce(
      o.transfer_to,
      case
        when o.tx_sender is not null and o.tx_sender is distinct from o.transfer_from then o.tx_sender
        else cast(null as varbinary)
      end,
      case
        when o.prev_owner is not null and o.prev_owner is distinct from o.transfer_from then o.prev_owner
        else cast(null as varbinary)
      end,
      o.transfer_from
    ) as to_resolved,
    case
      when o.transfer_to is not null then 'observed'
      when o.tx_sender is not null and o.tx_sender is distinct from o.transfer_from then 'derived_tx_sender'
      when o.prev_owner is not null and o.prev_owner is distinct from o.transfer_from then 'derived_prev_owner'
      when o.transfer_from is not null then 'mirrored_from'
      else 'unresolved_null'
    end as to_resolution_type,
    case
      when o.transfer_from is not null and o.transfer_to is not null then false
      else true
    end as is_counterparty_assumed,
    regexp_replace(
      case
        when starts_with(lower(split_part(o.coin_type, '::', 1)), '0x')
          then lower(split_part(o.coin_type, '::', 1))
        else concat('0x', lower(split_part(o.coin_type, '::', 1)))
      end,
      '^0x0*([0-9a-f]+)$',
      '0x$1'
    ) as contract_address,
    o.coin_type,
    o.coin_type_normalized,
    o.amount_raw,
    o.balance_delta,
    o.object_id,
    o.version,
    o.object_status,
    o.owner_type,
    o.coin_balance,
    o.prev_balance,
    o.prev_owner,
    o.has_ownership_change,
    case
      when o.owner_net_type = 'owner_residual_debit' then 'ownership_balance_spend'
      when o.owner_net_type = 'owner_residual_credit' then 'ownership_balance_topup'
      when o.object_status = 'Created' then 'object_created'
      when o.object_status = 'Deleted' then 'object_deleted'
      when o.has_ownership_change and o.balance_delta != 0 then 'transfer_with_balance_change'
      when o.has_ownership_change then 'ownership_transfer'
      when o.balance_delta > 0 then 'balance_increase'
      when o.balance_delta < 0 then 'balance_decrease'
      else 'other'
    end as transfer_type,
    case
      when o.owner_net_type in ('owner_residual_debit', 'owner_residual_credit')
        or o.object_status in ('Created', 'Deleted')
      then true
      else false
    end as is_supply_match_candidate,
    false as is_supply_event,
    cast(null as varchar) as supply_event_type,
    case
      when o.owner_net_type = 'owner_residual_debit' then 'debit'
      when o.owner_net_type = 'owner_residual_credit' then 'credit'
      when o.balance_delta > 0 then 'credit'
      when o.balance_delta < 0 then 'debit'
      else 'neutral'
    end as transfer_direction,
    r.tx_net_delta
  from object_transfers_source o
  left join tx_coin_reconciliation r
    on o.tx_digest = r.tx_digest
    and o.coin_type = r.coin_type
),

supply_match_counts as (
  select
    s.tx_digest,
    s.coin_type_normalized,
    s.amount_raw,
    case
      when s.supply_event_type = 'mint' then 'credit'
      when s.supply_event_type = 'burn' then 'debit'
      else cast(null as varchar)
    end as transfer_direction,
    count(*) as matched_supply_rows
  from supply_events s
  where s.supply_event_type in ('mint', 'burn')
  group by 1, 2, 3, 4
),

object_transfers_ranked as (
  select
    o.*,
    coalesce(m.matched_supply_rows, 0) as matched_supply_rows,
    case
      when o.is_supply_match_candidate then sum(
        case
          when o.is_supply_match_candidate then 1
          else 0
        end
      ) over (
        partition by o.tx_digest, o.coin_type_normalized, o.amount_raw, o.transfer_direction
        order by o.unique_key
        rows between unbounded preceding and current row
      )
      else cast(null as bigint)
    end as candidate_rank
  from object_transfers o
  left join supply_match_counts m
    on o.tx_digest = m.tx_digest
    and o.coin_type_normalized = m.coin_type_normalized
    and o.amount_raw = m.amount_raw
    and o.transfer_direction = m.transfer_direction
),

object_transfers_non_supply as (
  select r.*
  from object_transfers_ranked r
  where (r.is_supply_match_candidate and r.candidate_rank > r.matched_supply_rows)
    or not r.is_supply_match_candidate
),

all_transfers as (
  select
    o.unique_key,
    o.blockchain,
    o.block_month,
    o.block_date,
    o.block_time,
    o.checkpoint,
    o.tx_digest,
    o.token_standard,
    o.tx_from,
    o."from",
    o.to,
    o.from_resolved,
    o.from_resolution_type,
    o.to_resolved,
    o.to_resolution_type,
    o.is_counterparty_assumed,
    o.contract_address,
    o.coin_type,
    o.coin_type_normalized,
    o.amount_raw,
    o.balance_delta,
    o.object_id,
    o.version,
    o.object_status,
    o.owner_type,
    o.coin_balance,
    o.prev_balance,
    o.prev_owner,
    o.has_ownership_change,
    o.transfer_type,
    o.is_supply_event,
    o.supply_event_type,
    o.transfer_direction,
    o.tx_net_delta
  from object_transfers_non_supply o
  union all
  select
    s.unique_key,
    'sui' as blockchain,
    s.block_month,
    s.block_date,
    s.block_time,
    s.checkpoint,
    s.tx_digest,
    'sui_coin' as token_standard,
    s.tx_from,
    s."from",
    s.to,
    s."from" as from_resolved,
    'observed' as from_resolution_type,
    s.to as to_resolved,
    case
      when s.to is null then 'unresolved_null'
      else 'observed'
    end as to_resolution_type,
    case
      when s."from" is null or s.to is null then true
      else false
    end as is_counterparty_assumed,
    regexp_replace(
      case
        when starts_with(lower(split_part(s.coin_type, '::', 1)), '0x')
          then lower(split_part(s.coin_type, '::', 1))
        else concat('0x', lower(split_part(s.coin_type, '::', 1)))
      end,
      '^0x0*([0-9a-f]+)$',
      '0x$1'
    ) as contract_address,
    s.coin_type,
    s.coin_type_normalized,
    s.amount_raw,
    s.balance_delta,
    cast(null as varbinary) as object_id,
    cast(null as decimal(20, 0)) as version,
    cast(null as varchar) as object_status,
    cast(null as varchar) as owner_type,
    cast(null as decimal(38, 0)) as coin_balance,
    cast(null as decimal(38, 0)) as prev_balance,
    cast(null as varbinary) as prev_owner,
    cast(false as boolean) as has_ownership_change,
    s.supply_event_type as transfer_type,
    true as is_supply_event,
    s.supply_event_type,
    case
      when s.supply_event_type = 'mint' then 'credit'
      when s.supply_event_type = 'burn' then 'debit'
      else 'neutral'
    end as transfer_direction,
    cast(null as decimal(38, 0)) as tx_net_delta
  from supply_events s
)

select
  unique_key,
  blockchain,
  block_month,
  block_date,
  block_time,
  checkpoint,
  tx_digest,
  token_standard,
  tx_from,
  "from",
  to,
  from_resolved,
  from_resolution_type,
  to_resolved,
  to_resolution_type,
  is_counterparty_assumed,
  contract_address,
  coin_type,
  amount_raw,
  balance_delta,
  object_id,
  version,
  object_status,
  owner_type,
  coin_balance,
  prev_balance,
  prev_owner,
  has_ownership_change,
  transfer_type,
  is_supply_event,
  supply_event_type,
  transfer_direction,
  tx_net_delta,
  current_timestamp as _updated_at
from all_transfers
