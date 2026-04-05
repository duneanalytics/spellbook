{{
  config(
    schema = 'tokens_sui',
    alias = 'direct_transfers',
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

-- load object event deltas and mark transfers that need sender fallback
cross_address_precheck as (
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
    f.has_ownership_change,
    (f.prev_owner is null or f.prev_owner is distinct from f.receiver) as passes_cross_filter,
    (f.object_status = 'Created' or f.prev_owner is null) as needs_tx_sender
  from {{ ref('tokens_sui_object_event_deltas') }} f
  where f.block_date >= date '{{ sui_transfer_start_date }}'
    and (
      f.balance_delta != 0
      or f.has_ownership_change
    )
    {% if is_incremental() %}
    and {{ incremental_predicate('f.block_date') }}
    {% endif %}
),

-- load tx senders only for transfers that need sender fallback
required_tx_senders as (
  select
    t.transaction_digest as tx_digest,
    t.sender as tx_sender
  from {{ source('sui', 'transactions') }} t
  inner join (
    select distinct
      p.tx_digest
    from cross_address_precheck p
    where p.needs_tx_sender
      and (p.passes_cross_filter or p.object_status = 'Created')
      and p.tx_digest is not null
  ) d on t.transaction_digest = d.tx_digest
  where t.date >= date '{{ sui_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('t.date') }}
    {% endif %}
),

-- build cross-address transfers with sender fallback when needed
cross_address_transfers as (
  select
    p.object_id,
    p.version,
    p.tx_digest,
    p.timestamp_ms,
    p.block_date,
    p.block_month,
    p.checkpoint,
    p.owner_type,
    p.receiver,
    p.coin_type,
    p.object_status,
    p.coin_balance,
    p.prev_owner,
    p.prev_balance,
    p.balance_delta,
    p.has_ownership_change,
    cast(null as varbinary) as tx_sender
  from cross_address_precheck p
  where p.passes_cross_filter
    and not p.needs_tx_sender
  union all
  select
    p.object_id,
    p.version,
    p.tx_digest,
    p.timestamp_ms,
    p.block_date,
    p.block_month,
    p.checkpoint,
    p.owner_type,
    p.receiver,
    p.coin_type,
    p.object_status,
    p.coin_balance,
    p.prev_owner,
    p.prev_balance,
    p.balance_delta,
    p.has_ownership_change,
    s.tx_sender
  from cross_address_precheck p
  left join required_tx_senders s
    on p.tx_digest = s.tx_digest
  where p.needs_tx_sender
    and (
      (
        p.object_status = 'Created'
        and s.tx_sender is not null
        and s.tx_sender is distinct from p.receiver
      )
      or (
        p.object_status != 'Created'
        and coalesce(p.prev_owner, s.tx_sender) is distinct from p.receiver
      )
    )
)

select
  {{ dbt_utils.generate_surrogate_key([
    'f.tx_digest',
    'f.coin_type',
    "case when f.object_status = 'Created' then f.tx_sender else coalesce(f.prev_owner, f.tx_sender) end",
    'f.receiver',
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
  cast('direct' as varchar) as owner_net_type,
  case
    when f.object_status = 'Created' then f.tx_sender
    else coalesce(f.prev_owner, f.tx_sender)
  end as transfer_from,
  f.receiver as transfer_to,
  case
    when f.object_status = 'Created' or f.has_ownership_change then f.coin_balance
    else abs(f.balance_delta)
  end as amount_raw,
  current_timestamp as _updated_at
from cross_address_transfers f
