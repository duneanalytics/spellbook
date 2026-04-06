{{
  config(
    schema = 'tokens_aptos',
    alias = 'base_transfers',
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
{% set canonical_usdc_asset_type = '0xbae207659db88bea0cbead6da0ed00aac12edcdda169e591cd41c94180b46f3b' %}

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

event_balances as (
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
    sum(
      case
        when transfer_direction = 'debit' then -amount_raw
        else amount_raw
      end
    ) over (
      partition by tx_version, asset_type
      order by event_index
      rows between unbounded preceding and current row
    ) as balance_tracker
  from transfer_events
),

session_events as (
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
  from event_balances
),

session_sums as (
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
  from session_events
),

paired_transfers as (
  select
    {{ dbt_utils.generate_surrogate_key(['tx_version', 'withdraw_event_index', 'deposit_event_index']) }} as unique_key,
    tx_version,
    tx_hash,
    block_date,
    block_time,
    block_month,
    withdraw_event_index as event_index,
    deposit_event_index as counterpart_event_index,
    from_address,
    to_address,
    from_storage_id,
    to_storage_id,
    asset_type,
    token_standard,
    amount_raw,
    'transfer' as transfer_type,
    current_timestamp as _updated_at
  from (
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
    from session_sums w
    inner join session_sums d
      on w.tx_version = d.tx_version
      and w.asset_type = d.asset_type
      and w.session_id = d.session_id
      and w.transfer_direction = 'debit'
      and d.transfer_direction = 'credit'
    where w.amount_csum > coalesce(d.amount_csum_prev, cast(0 as uint256))
      and d.amount_csum > coalesce(w.amount_csum_prev, cast(0 as uint256))
  ) matched_pairs
  where amount_raw > cast(0 as uint256)
),

paired_event_amounts as (
  select
    tx_version,
    event_index,
    sum(amount_raw) as paired_amount_raw
  from (
    select
      tx_version,
      event_index,
      amount_raw
    from paired_transfers
    union all
    select
      tx_version,
      counterpart_event_index as event_index,
      amount_raw
    from paired_transfers
  ) paired_event_amounts_raw
  group by 1, 2
),

residual_transfers as (
  select
    {{ dbt_utils.generate_surrogate_key(['e.tx_version', 'e.event_index']) }} as unique_key,
    e.tx_version,
    e.tx_hash,
    e.block_date,
    e.block_time,
    e.block_month,
    e.event_index,
    cast(null as bigint) as counterpart_event_index,
    case
      -- Alignment shim: map one-sided canonical USDC residuals to self-attributed endpoints.
      when e.asset_type = '{{ canonical_usdc_asset_type }}' then e.owner_address
      when e.transfer_direction = 'credit' then cast(null as varbinary)
      else e.owner_address
    end as from_address,
    case
      when e.asset_type = '{{ canonical_usdc_asset_type }}' then e.owner_address
      when e.transfer_direction = 'credit' then e.owner_address
      else cast(null as varbinary)
    end as to_address,
    case
      when e.asset_type = '{{ canonical_usdc_asset_type }}' then e.storage_id
      when e.transfer_direction = 'credit' then cast(null as varbinary)
      else e.storage_id
    end as from_storage_id,
    case
      when e.asset_type = '{{ canonical_usdc_asset_type }}' then e.storage_id
      when e.transfer_direction = 'credit' then e.storage_id
      else cast(null as varbinary)
    end as to_storage_id,
    e.asset_type,
    e.token_standard,
    e.amount_raw - coalesce(p.paired_amount_raw, cast(0 as uint256)) as amount_raw,
    case
      when e.transfer_direction = 'credit' then 'mint'
      else 'burn'
    end as transfer_type,
    current_timestamp as _updated_at
  from transfer_events e
  left join paired_event_amounts p
    on e.tx_version = p.tx_version
    and e.event_index = p.event_index
  where e.amount_raw > coalesce(p.paired_amount_raw, cast(0 as uint256))
)

select
  unique_key,
  tx_version,
  tx_hash,
  block_date,
  block_time,
  block_month,
  event_index,
  counterpart_event_index,
  from_address,
  to_address,
  from_storage_id,
  to_storage_id,
  asset_type,
  case
    when split_part(asset_type, '::', 1) is null then cast(null as varbinary)
    else from_hex(
      '0x' || lpad(
        ltrim(split_part(asset_type, '::', 1), '0x'),
        64,
        '0'
      )
    )
  end as contract_address,
  token_standard,
  amount_raw,
  transfer_type,
  _updated_at
from (
  select * from paired_transfers
  union all
  select * from residual_transfers
) t
