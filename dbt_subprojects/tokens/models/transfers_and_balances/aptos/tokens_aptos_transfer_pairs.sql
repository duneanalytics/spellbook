{{
  config(
    schema = 'tokens_aptos',
    alias = 'transfer_pairs',
    materialized = 'incremental',
    file_format = 'delta',
    partition_by = ['block_month'],
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    merge_skip_unchanged = true
  )
}}

{% set aptos_transfer_start_date = '2026-01-01' %} -- ci test only

with transfer_events as (
  select
    tx_version,
    tx_hash,
    block_date,
    block_time,
    block_month,
    event_index,
    owner_address,
    storage_id,
    asset_type,
    token_standard,
    amount_raw,
    transfer_direction
  from {{ ref('tokens_aptos_transfer_events') }}
  where block_date >= date '{{ aptos_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('block_time') }}
    {% endif %}
),

event_sessions as (
  select
    tx_version,
    tx_hash,
    block_date,
    block_time,
    block_month,
    event_index,
    owner_address,
    storage_id,
    asset_type,
    token_standard,
    amount_raw,
    transfer_direction,
    case
      when transfer_direction = 'debit' then -amount_raw
      else amount_raw
    end as net_amount
  from transfer_events
),

cumulative as (
  select
    tx_version,
    tx_hash,
    block_date,
    block_time,
    block_month,
    event_index,
    owner_address,
    storage_id,
    asset_type,
    token_standard,
    amount_raw,
    transfer_direction,
    sum(net_amount) over (
      partition by tx_version, asset_type
      order by event_index
      rows between unbounded preceding and current row
    ) as balance_tracker
  from event_sessions
),

sessioning as (
  select
    tx_version,
    tx_hash,
    block_date,
    block_time,
    block_month,
    event_index,
    owner_address,
    storage_id,
    asset_type,
    token_standard,
    amount_raw,
    transfer_direction,
    coalesce(
      sum(
        if(balance_tracker >= 0, 1, 0)
      ) over (
        partition by tx_version, asset_type
        order by event_index
        rows between unbounded preceding and 1 preceding
      ),
      0
    ) as session_id
  from cumulative
),

session_sum as (
  select
    tx_version,
    tx_hash,
    block_date,
    block_time,
    block_month,
    event_index,
    owner_address,
    storage_id,
    asset_type,
    token_standard,
    amount_raw,
    transfer_direction,
    session_id,
    sum(amount_raw) over (
      partition by tx_version, asset_type, session_id, transfer_direction
      order by event_index
      rows between unbounded preceding and current row
    ) as amount_csum,
    sum(amount_raw) over (
      partition by tx_version, asset_type, session_id, transfer_direction
      order by event_index
      rows between unbounded preceding and 1 preceding
    ) as amount_csum_prev
  from sessioning
),

withdraw_events as (
  select
    tx_version,
    tx_hash,
    block_date,
    block_time,
    block_month,
    event_index,
    owner_address,
    storage_id,
    asset_type,
    token_standard,
    amount_raw,
    session_id,
    amount_csum,
    amount_csum_prev
  from session_sum
  where transfer_direction = 'debit'
),

deposit_events as (
  select
    tx_version,
    tx_hash,
    block_date,
    block_time,
    block_month,
    event_index,
    owner_address,
    storage_id,
    asset_type,
    token_standard,
    amount_raw,
    session_id,
    amount_csum,
    amount_csum_prev
  from session_sum
  where transfer_direction = 'credit'
),

transfers_multi_fifo as (
  select
    w.tx_version,
    w.tx_hash,
    w.block_date,
    w.block_time,
    w.block_month,
    w.event_index as withdraw_event_index,
    d.event_index as deposit_event_index,
    w.owner_address as from_address,
    d.owner_address as to_address,
    w.storage_id as from_storage_id,
    d.storage_id as to_storage_id,
    w.asset_type,
    w.token_standard,
    least(
      least(
        d.amount_csum - coalesce(w.amount_csum_prev, cast(0 as uint256)),
        d.amount_raw
      ),
      least(
        w.amount_csum - coalesce(d.amount_csum_prev, cast(0 as uint256)),
        w.amount_raw
      )
    ) as amount_raw
  from withdraw_events w
  inner join deposit_events d
    on w.tx_version = d.tx_version
    and w.asset_type = d.asset_type
    and w.session_id = d.session_id
  where w.amount_csum > coalesce(d.amount_csum_prev, cast(0 as uint256))
    and d.amount_csum > coalesce(w.amount_csum_prev, cast(0 as uint256))
)

select
  {{ dbt_utils.generate_surrogate_key(['tx_version', 'withdraw_event_index', 'deposit_event_index']) }} as unique_key,
  tx_version,
  tx_hash,
  block_date,
  block_time,
  block_month,
  withdraw_event_index,
  deposit_event_index,
  from_address,
  to_address,
  from_storage_id,
  to_storage_id,
  asset_type,
  token_standard,
  amount_raw,
  'transfer' as transfer_type,
  current_timestamp as _updated_at
from transfers_multi_fifo
where amount_raw > cast(0 as uint256)
