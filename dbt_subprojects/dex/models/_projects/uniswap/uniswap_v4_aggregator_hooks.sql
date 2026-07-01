{{ config(
    schema = 'uniswap_v4'
    , alias = 'aggregator_hooks'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'address']
    )
}}

-- Registry of Uniswap V4 BaseAggregatorHook contracts.
-- These hooks hold no V4 liquidity: in beforeSwap they route the entire swap to an
-- external DEX (Curve / Aerodrome / PancakeSwap / etc.) and return the full delta,
-- so their swaps must be classified as aggregator trades, not venue trades.
-- A contract is a BaseAggregatorHook iff its deployed bytecode dispatches BOTH
-- pollTokenJar() (0x3f7d1179) AND pseudoTotalValueLocked(bytes32) (0x2d910fb4);
-- matching the PUSH4-prefixed selectors keeps the check deterministic and
-- self-maintaining (no static address list).

{% set chains = uniswap_v4_chains() %}

{% for chain in chains %}
select
    '{{ chain }}' as blockchain
    , address
    , min(block_time) as created_at
from {{ source(chain, 'creation_traces') }}
where bytearray_position(code, 0x633f7d1179) > 0   -- PUSH4 + pollTokenJar()
  and bytearray_position(code, 0x632d910fb4) > 0   -- PUSH4 + pseudoTotalValueLocked(bytes32)
  and address is not null
  -- BaseAggregatorHook takes the PoolManager in its constructor; the earliest V4
  -- PoolManager deploy is unichain, late Dec 2024 — no aggregator hook can predate this
  and block_time >= timestamp '2024-11-01'
{%- if is_incremental() %}
  -- deliberately wider than incremental_predicate (3d): a creation_traces ingestion
  -- lag beyond the lookback would permanently miss a hook; this scan is tiny
  and block_time >= now() - interval '30' day
{%- endif %}
-- metamorphic CREATE2 redeploys can emit two creation rows for one address;
-- duplicate source keys would fail the Trino MERGE
group by 1, 2
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
