-- Reconciliation invariant for XRPL affected-node extraction.
--
-- Purpose:
-- Verify each recent transaction emits exactly one `tokens_xrpl_affected_nodes`
-- row per raw `metadata.AffectedNodes` entry, with contiguous zero-based
-- `node_index` values.
--
-- Failure interpretation:
-- Any returned row means affected-node unnesting drifted, which can corrupt
-- downstream node selection and transfer reconstruction.

with transaction_scope as (
  select
    t.block_date,
    t.tx_hash,
    case
      when json_extract(t.metadata, '$.AffectedNodes') is null then 0
      else cardinality(cast(json_extract(t.metadata, '$.AffectedNodes') as array(json)))
    end as expected_node_count
  from {{ ref('tokens_xrpl_transaction_metadata') }} t
  where {{ incremental_predicate('t.block_time') }}
),

actual_node_counts as (
  select
    n.block_date,
    n.tx_hash,
    count(*) as actual_node_count,
    count(distinct n.node_index) as distinct_node_count,
    min(n.node_index) as min_node_index,
    max(n.node_index) as max_node_index
  from {{ ref('tokens_xrpl_affected_nodes') }} n
  where {{ incremental_predicate('n.block_time') }}
  group by 1, 2
)

select
  coalesce(t.block_date, a.block_date) as block_date,
  coalesce(t.tx_hash, a.tx_hash) as tx_hash,
  t.expected_node_count,
  a.actual_node_count,
  a.distinct_node_count,
  a.min_node_index,
  a.max_node_index
from transaction_scope t
full outer join actual_node_counts a
  on t.block_date = a.block_date
  and t.tx_hash = a.tx_hash
where coalesce(t.expected_node_count, 0) != coalesce(a.actual_node_count, 0)
  or coalesce(a.actual_node_count, 0) != coalesce(a.distinct_node_count, 0)
  or (
    coalesce(a.actual_node_count, 0) > 0
    and (
      a.min_node_index is distinct from 0
      or a.max_node_index is distinct from a.actual_node_count - 1
    )
  )
