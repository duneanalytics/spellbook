{{
  config(
    schema = 'tokens_xrpl',
    alias = 'affected_nodes',
    materialized = 'incremental',
    file_format = 'delta',
    partition_by = ['block_month'],
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    merge_skip_unchanged = true,
  )
}}

{% set xrpl_transfer_start_date = '2026-01-01' %} -- ci test, revert to '2012-06-01'

with txs as (
  select
    *
  from {{ ref('tokens_xrpl_transaction_metadata') }}
  where block_date >= date '{{ xrpl_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('block_date') }}
    {% endif %}
),

unnested_nodes as (
  select
    t.tx_hash,
    t.blockchain,
    t.block_month,
    t.block_date,
    t.block_time,
    t.block_number,
    t.tx_from,
    t.tx_to,
    t.tx_index,
    t.transaction_type,
    t.transaction_result,
    node_ordinality - 1 as node_index,
    case
      when json_extract(node, '$.CreatedNode') is not null then 'CreatedNode'
      when json_extract(node, '$.ModifiedNode') is not null then 'ModifiedNode'
      when json_extract(node, '$.DeletedNode') is not null then 'DeletedNode'
      else cast(null as varchar)
    end as node_action,
    coalesce(
      json_extract(node, '$.CreatedNode'),
      json_extract(node, '$.ModifiedNode'),
      json_extract(node, '$.DeletedNode')
    ) as node_payload
  from txs t
  cross join unnest(
    cast(json_extract(t.metadata, '$.AffectedNodes') as array(json))
  ) with ordinality as n (node, node_ordinality)
  where json_extract(t.metadata, '$.AffectedNodes') is not null
)

select
  {{ dbt_utils.generate_surrogate_key(['tx_hash', 'node_index']) }} as unique_key,
  tx_hash,
  blockchain,
  block_month,
  block_date,
  block_time,
  block_number,
  tx_from,
  tx_to,
  tx_index,
  transaction_type,
  transaction_result,
  node_index,
  node_action,
  json_extract_scalar(node_payload, '$.LedgerEntryType') as ledger_entry_type,
  json_extract_scalar(node_payload, '$.LedgerIndex') as ledger_entry_id,
  json_format(json_extract(node_payload, '$.FinalFields')) as final_fields,
  json_format(json_extract(node_payload, '$.PreviousFields')) as previous_fields,
  json_format(json_extract(node_payload, '$.NewFields')) as new_fields,
  current_timestamp as _updated_at
from unnested_nodes
