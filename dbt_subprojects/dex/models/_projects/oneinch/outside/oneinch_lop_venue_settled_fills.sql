{%- set stream = oneinch_lo_cfg_macro() -%}

{{
    config(
        schema = 'oneinch',
        alias = 'lop_venue_settled_fills',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        partition_by = ['blockchain', 'block_month'],
        unique_key = ['blockchain', 'block_month', 'block_date', 'execution_id'],
        post_hook = '{{ hide_spells() }}'
    )
}}

{% set window_guard %}
        {% if var('dev_dates', false) -%} and block_date > current_date - interval '3' day
        {%- elif is_incremental() -%} and {{ incremental_predicate('block_time') }} {%- endif %}
{% endset %}

-- LOP fills that a resolver settled on an underlying DEX in the same tx: the venue's
-- own row is already in dex_<blockchain>_base_trades, so keeping the '1inch-LOP' row
-- in dex.trades double-counts the same fill. Fills settled via private liquidity
-- (no co-occurring venue row) stay in dex.trades; fills listed here are reclassified
-- into dex_aggregator.trades (see oneinch_lop_aggregator_trades).
--
-- Keep-guards (fills NOT listed here even when a venue row co-occurs):
--   * fills nested under a classic / fusion / cross-chain execution of the same tx:
--     route legs of an aggregator swap or liquidity for a settlement order — real
--     maker liquidity whose parent trade is accounted for in dex_aggregator.trades
--   * direct fills (flags['direct']): their synthesized second-side row already
--     feeds dex_aggregator.trades as '1inch', so reclassifying them would duplicate
--     within that table; they remain in dex.trades unconditionally
--
-- Cross-chain identical (tx_hash, call_trace_address) replays are an accepted
-- ~0-probability residual (the evt_index window is tx_hash-only).

with

fills as (
    select
        blockchain
        , block_month
        , block_date
        , block_time
        , tx_hash
        , call_trace_address
        , execution_id
        , flags
        , {{ oneinch_lop_evt_index() }} as evt_index -- numbered before the exclusions below
    from {{ ref('oneinch_swaps') }}
    where true
        and mode = 'limits'
        and tx_success
        and call_success
        {{ window_guard }}
)

, parent_calls as (
    select
        blockchain
        , tx_hash
        , call_trace_address
    from {{ ref('oneinch_swaps') }}
    where true
        and mode in ('classic', 'fusion', 'cross-chain')
        and tx_success
        and call_success
        and not coalesce(element_at(flags, 'second_side'), false) -- second-side rows are relabeled LOP fills, not routing calls
        {{ window_guard }}
)

, lop_tx_keys as (
    select distinct
        blockchain
        , block_month
        , tx_hash
    from fills
)

-- joined to the small LOP tx set (instead of EXISTS with the union as the build side)
-- so the base trades scans get dynamically filtered on tx_hash
, venue_txs as (
    select distinct
        k.blockchain
        , k.block_month
        , k.tx_hash
    from (
        {%- for blockchain in oneinch_blockchains_cfg_macro() if blockchain.exposed and stream.name in blockchain.exposed %}
        select
            '{{ blockchain.name }}' as blockchain
            , block_month
            , tx_hash
        from {{ ref('dex_' + blockchain.name + '_base_trades') }}
        where true
            and block_time >= timestamp '{{ stream.start }}' -- LOP stream start; prunes pre-LOP history on full builds
            {{ window_guard }}
        {% if not loop.last %} union all {% endif %}
        {%- endfor %}
    ) as b
    join lop_tx_keys as k
        on k.blockchain = b.blockchain
        and k.block_month = b.block_month
        and k.tx_hash = b.tx_hash
)

-- output --

select
    f.blockchain
    , f.block_month
    , f.block_date
    , f.block_time
    , f.tx_hash
    , f.call_trace_address
    , f.execution_id
    , f.evt_index
from fills as f
where true
    and not coalesce(element_at(f.flags, 'direct'), false)
    and not exists ( -- keep-guard: fill is nested under a parent execution of the same tx
        select 1
        from parent_calls as p
        where true
            and p.blockchain = f.blockchain
            and p.tx_hash = f.tx_hash
            and cardinality(f.call_trace_address) > cardinality(p.call_trace_address) -- strict: a direct fill's second-side row shares its call_trace_address
            and slice(f.call_trace_address, 1, cardinality(p.call_trace_address)) = p.call_trace_address
    )
    and exists ( -- a venue's own trade row co-occurs in the same tx
        select 1
        from venue_txs as v
        where true
            and v.blockchain = f.blockchain
            and v.block_month = f.block_month
            and v.tx_hash = f.tx_hash
    )
