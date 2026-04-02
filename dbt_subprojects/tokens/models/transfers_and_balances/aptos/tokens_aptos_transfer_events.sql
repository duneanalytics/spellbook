{{
  config(
    schema = 'tokens_aptos',
    alias = 'transfer_events',
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

with source_events as (
  select
    a.tx_version,
    a.tx_hash,
    a.block_date,
    a.block_time,
    a.block_month,
    a.event_index,
    a.event_type,
    a.owner_address,
    a.storage_id,
    a.asset_type,
    a.token_standard,
    cast(a.amount as uint256) as amount_raw
  from {{ source('aptos_fungible_asset', 'activities') }} a
  where a.amount > 0
    and a.block_date >= date '{{ aptos_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('a.block_time') }}
    {% endif %}
)

select
  {{ dbt_utils.generate_surrogate_key(['tx_version', 'event_index']) }} as unique_key,
  tx_version,
  tx_hash,
  block_date,
  block_time,
  block_month,
  event_index,
  event_type,
  case
    when event_type in ('0x1::coin::WithdrawEvent', '0x1::fungible_asset::WithdrawEvent', '0x1::fungible_asset::Withdraw') then 'withdraw'
    when event_type in ('0x1::coin::DepositEvent', '0x1::fungible_asset::DepositEvent', '0x1::fungible_asset::Deposit') then 'deposit'
  end as activity_type,
  case
    when event_type in ('0x1::coin::WithdrawEvent', '0x1::fungible_asset::WithdrawEvent', '0x1::fungible_asset::Withdraw') then 'debit'
    when event_type in ('0x1::coin::DepositEvent', '0x1::fungible_asset::DepositEvent', '0x1::fungible_asset::Deposit') then 'credit'
  end as transfer_direction,
  owner_address,
  storage_id,
  asset_type,
  token_standard,
  amount_raw,
  current_timestamp as _updated_at
from source_events
