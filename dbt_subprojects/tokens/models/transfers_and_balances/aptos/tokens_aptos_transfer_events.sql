{{
  config(
    schema = 'tokens_aptos',
    alias = 'transfer_events',
    materialized = 'incremental',
    file_format = 'delta',
    partition_by = ['block_date'],
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    merge_skip_unchanged = true
  )
}}

{% set aptos_transfer_start_date = '2022-10-12' %}

-- centralize supported event filtering and normalized labels in one place
with event_type_map as (
  select
    event_type,
    activity_type,
    transfer_direction
  from (
    values
      ('0x1::coin::WithdrawEvent', 'withdraw', 'debit'),
      ('0x1::fungible_asset::WithdrawEvent', 'withdraw', 'debit'),
      ('0x1::fungible_asset::Withdraw', 'withdraw', 'debit'),
      ('0x1::coin::DepositEvent', 'deposit', 'credit'),
      ('0x1::fungible_asset::DepositEvent', 'deposit', 'credit'),
      ('0x1::fungible_asset::Deposit', 'deposit', 'credit')
  ) as t (event_type, activity_type, transfer_direction)
),

source_events as (
  select
    a.tx_version,
    a.tx_hash,
    a.block_date,
    a.block_time,
    a.block_month,
    a.event_index,
    a.event_type,
    m.activity_type,
    m.transfer_direction,
    a.owner_address,
    a.storage_id,
    a.asset_type,
    a.token_standard,
    cast(a.amount as uint256) as amount_raw
  from {{ ref('aptos_fungible_asset_activities') }} a
  inner join event_type_map m
    on a.event_type = m.event_type
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
  activity_type,
  transfer_direction,
  owner_address,
  storage_id,
  asset_type,
  token_standard,
  amount_raw,
  current_timestamp as _updated_at
from source_events
