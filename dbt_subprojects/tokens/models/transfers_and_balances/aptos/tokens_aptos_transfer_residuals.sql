{{
  config(
    schema = 'tokens_aptos',
    alias = 'transfer_residuals',
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
  select *
  from {{ ref('tokens_aptos_transfer_events') }}
  where block_date >= date '{{ aptos_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('block_time') }}
    {% endif %}
),

paired_event_indexes as (
  select
    tx_version,
    withdraw_event_index as event_index
  from {{ ref('tokens_aptos_transfer_pairs') }}
  where block_date >= date '{{ aptos_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('block_time') }}
    {% endif %}
  union all
  select
    tx_version,
    deposit_event_index as event_index
  from {{ ref('tokens_aptos_transfer_pairs') }}
  where block_date >= date '{{ aptos_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('block_time') }}
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
  case
    when e.transfer_direction = 'credit' then cast(null as varbinary)
    else e.owner_address
  end as from_address,
  case
    when e.transfer_direction = 'credit' then e.owner_address
    else cast(null as varbinary)
  end as to_address,
  case
    when e.transfer_direction = 'credit' then cast(null as varbinary)
    else e.storage_id
  end as from_storage_id,
  case
    when e.transfer_direction = 'credit' then e.storage_id
    else cast(null as varbinary)
  end as to_storage_id,
  e.asset_type,
  e.token_standard,
  e.amount_raw,
  case
    when e.transfer_direction = 'credit' then 'mint'
    else 'burn'
  end as transfer_type,
  current_timestamp as _updated_at
from transfer_events e
left join paired_event_indexes p
  on e.tx_version = p.tx_version
  and e.event_index = p.event_index
where p.event_index is null
