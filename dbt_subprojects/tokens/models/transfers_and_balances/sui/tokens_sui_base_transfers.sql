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

-- temporary ci filter: original start date '2023-04-12', bumped to '2026-01-01' to reduce scan and unblock ci run
{% set sui_transfer_start_date = '2026-01-01' %}

with

owner_net_transfers as (
  select
    f.unique_key,
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
    f.amount_raw
  from {{ ref('tokens_sui_owner_net_transfers') }} f
  where f.block_date >= date '{{ sui_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('f.block_date') }}
    {% endif %}
),

transfer_event_features as (
  select
    f.tx_digest,
    f.coin_type,
    f.balance_delta,
    f.receiver,
    f.prev_owner,
    f.has_ownership_change
  from {{ ref('tokens_sui_object_event_deltas') }} f
  where f.block_date >= date '{{ sui_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('f.block_date') }}
    {% endif %}
),

transfer_event_candidates as (
  select
    f.tx_digest,
    f.coin_type,
    f.balance_delta,
    f.receiver,
    f.prev_owner
  from transfer_event_features f
  where f.balance_delta != 0
    or f.has_ownership_change
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

tx_coin_reconciliation as (
  select
    f.tx_digest,
    f.coin_type,
    sum(cast(f.balance_delta as decimal(38, 0))) as tx_net_delta
  from transfer_event_candidates f
  group by 1, 2
)

select
  f.unique_key,
  'sui' as blockchain,
  f.block_month,
  f.block_date,
  from_unixtime(f.timestamp_ms / 1000) as block_time,
  f.checkpoint,
  f.tx_digest,
  'sui_coin' as token_standard,
  f.tx_sender as tx_from,
  f.row_from as "from",
  f.row_to as to,
  coalesce(
    f.row_from,
    case
      when f.tx_sender is not null and f.tx_sender is distinct from f.row_to then f.tx_sender
      else cast(null as varbinary)
    end,
    case
      when f.prev_owner is not null and f.prev_owner is distinct from f.row_to then f.prev_owner
      else cast(null as varbinary)
    end,
    f.row_to
  ) as from_resolved,
  case
    when f.row_from is not null then 'observed'
    when f.tx_sender is not null and f.tx_sender is distinct from f.row_to then 'derived_tx_sender'
    when f.prev_owner is not null and f.prev_owner is distinct from f.row_to then 'derived_prev_owner'
    when f.row_to is not null then 'mirrored_to'
    else 'unresolved_null'
  end as from_resolution_type,
  coalesce(
    f.row_to,
    case
      when f.tx_sender is not null and f.tx_sender is distinct from f.row_from then f.tx_sender
      else cast(null as varbinary)
    end,
    case
      when f.prev_owner is not null and f.prev_owner is distinct from f.row_from then f.prev_owner
      else cast(null as varbinary)
    end,
    f.row_from
  ) as to_resolved,
  case
    when f.row_to is not null then 'observed'
    when f.tx_sender is not null and f.tx_sender is distinct from f.row_from then 'derived_tx_sender'
    when f.prev_owner is not null and f.prev_owner is distinct from f.row_from then 'derived_prev_owner'
    when f.row_from is not null then 'mirrored_from'
    else 'unresolved_null'
  end as to_resolution_type,
  case
    when f.row_from is not null and f.row_to is not null then false
    else true
  end as is_counterparty_assumed,
  regexp_replace(
    case
      when starts_with(lower(split_part(f.coin_type, '::', 1)), '0x')
        then lower(split_part(f.coin_type, '::', 1))
      else concat('0x', lower(split_part(f.coin_type, '::', 1)))
    end,
    '^0x0*([0-9a-f]+)$',
    '0x$1'
  ) as contract_address,
  f.coin_type as contract_address_full,
  f.amount_raw,
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
    when f.owner_net_leg = 'owner_residual_debit' then 'ownership_balance_spend'
    when f.owner_net_leg = 'owner_residual_credit' then 'ownership_balance_topup'
    when f.object_status = 'Created' then 'object_created'
    when f.object_status = 'Deleted' then 'object_deleted'
    when f.has_ownership_change and f.balance_delta != 0 then 'transfer_with_balance_change'
    when f.has_ownership_change then 'ownership_transfer'
    when f.balance_delta > 0 then 'balance_increase'
    when f.balance_delta < 0 then 'balance_decrease'
    else 'other'
  end as transfer_type,
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
    when f.owner_net_leg = 'owner_residual_debit' then 'debit'
    when f.owner_net_leg = 'owner_residual_credit' then 'credit'
    when f.balance_delta > 0 then 'credit'
    when f.balance_delta < 0 then 'debit'
    else 'neutral'
  end as transfer_direction,
  r.tx_net_delta,
  current_timestamp as _updated_at
from owner_net_transfers f
left join tx_coin_reconciliation r
  on f.tx_digest = r.tx_digest
  and f.coin_type = r.coin_type
left join supply_signals supply
  on f.tx_digest = supply.tx_digest
  and lower(f.coin_type) = supply.coin_type
