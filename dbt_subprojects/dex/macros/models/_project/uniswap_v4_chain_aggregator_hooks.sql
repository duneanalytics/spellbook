{% macro uniswap_v4_chain_aggregator_hooks(blockchain) %}
{#- Per-chain BaseAggregatorHook registry. Used only by
    uniswap_v4_{chain}_aggregator_hooks models. The cross-chain
    uniswap_v4.aggregator_hooks model keeps its own inline copy so a change
    here does not mark that shared table as state:modified.macros. -#}
select
    '{{ blockchain }}' as blockchain
    , address
    , min(block_time) as created_at
from {{ source(blockchain, 'creation_traces') }}
where bytearray_position(code, 0x633f7d1179) > 0   -- PUSH4 + pollTokenJar()
  and bytearray_position(code, 0x632d910fb4) > 0   -- PUSH4 + pseudoTotalValueLocked(bytes32)
  and address is not null
  -- BaseAggregatorHook takes the PoolManager in its constructor; the earliest V4
  -- PoolManager deploy is unichain, late Dec 2024, so no aggregator hook can predate this
  and block_time >= timestamp '2024-11-01'
{% if is_incremental() -%}
  -- deliberately wider than incremental_predicate (3d): a creation_traces ingestion
  -- lag beyond the lookback would permanently miss a hook; this scan is tiny
  and block_time >= now() - interval '30' day
{% endif %}
-- metamorphic CREATE2 redeploys can emit two creation rows for one address;
-- duplicate source keys would fail the Trino MERGE
group by 1, 2
{% endmacro %}
