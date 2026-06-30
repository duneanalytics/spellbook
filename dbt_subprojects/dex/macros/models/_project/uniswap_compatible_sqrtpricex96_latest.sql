{#
    Deep-history seed companion for uniswap_compatible_v4_liquidity_sqrtpricex96.

    Maintains, per pool id, the latest sqrtPriceX96 price-change row among events
    that are OLDER than `aged_days` (the "deep tail"). The price builder reads this
    tiny table (~one row per pool) instead of self-scanning all of its own history
    to recover each active pool's previous price.

    Why aged-only: the builder seeds previous_block_index_sum from the latest price
    strictly BEFORE its reload window. A pool active now can have its previous price
    arbitrarily far back (sqrtpricex96 is sparse/event-grain, not a forward-filled
    snapshot), so a bounded {{this}} re-read alone would drop the seed for
    dormant-then-active pools. This companion holds that deep tail; the builder
    unions it with a bounded recent {{this}} re-read. The companion never holds a
    reload-window row (aged_days > the model's reload window), so it composes with
    the builder's anti-join without double-counting.
#}
{% macro uniswap_compatible_v4_sqrtpricex96_latest(
    blockchain = null
    , project = 'uniswap'
    , version = '4'
    , PoolManager_evt_Initialize = null
    , PoolManager_evt_Swap = null
    , transactions = null
    , aged_days = 7
    , ingest_lookback_days = 10
    )
%}

with

aged_events as (
    select
        e.id
        , e.evt_block_time as block_time
        , e.evt_block_number as block_number
        , e.evt_index
        , {{ uniswap_compatible_v4_block_index_sum('e.evt_block_number', 'coalesce(e.evt_tx_index, tx.index)', 'e.evt_index') }} as block_index_sum
        , e.sqrtpricex96
    from {{ PoolManager_evt_Initialize }} e
    left join {{ transactions }} tx
        on e.evt_tx_index is null
        and e.evt_tx_hash = tx.hash
        and e.evt_block_number = tx.block_number
        and e.evt_block_date = tx.block_date
        and tx.block_time < current_date - interval '{{ aged_days }}' day
        {%- if is_incremental() %}
        and tx.block_time >= current_date - interval '{{ ingest_lookback_days }}' day
        {%- endif %}
    where e.sqrtPriceX96 is not null
        and e.evt_block_time < current_date - interval '{{ aged_days }}' day
        {%- if is_incremental() %}
        and e.evt_block_time >= current_date - interval '{{ ingest_lookback_days }}' day
        {%- elif target.name == 'ci' %}
        and e.evt_block_time >= current_date - interval '14' day
        {%- endif %}

    union all

    select
        e.id
        , e.evt_block_time as block_time
        , e.evt_block_number as block_number
        , e.evt_index
        , {{ uniswap_compatible_v4_block_index_sum('e.evt_block_number', 'coalesce(e.evt_tx_index, tx.index)', 'e.evt_index') }} as block_index_sum
        , e.sqrtpricex96
    from {{ PoolManager_evt_Swap }} e
    left join {{ transactions }} tx
        on e.evt_tx_index is null
        and e.evt_tx_hash = tx.hash
        and e.evt_block_number = tx.block_number
        and e.evt_block_date = tx.block_date
        and tx.block_time < current_date - interval '{{ aged_days }}' day
        {%- if is_incremental() %}
        and tx.block_time >= current_date - interval '{{ ingest_lookback_days }}' day
        {%- endif %}
    where e.sqrtPriceX96 is not null
        and e.evt_block_time < current_date - interval '{{ aged_days }}' day
        {%- if is_incremental() %}
        and e.evt_block_time >= current_date - interval '{{ ingest_lookback_days }}' day
        {%- elif target.name == 'ci' %}
        and e.evt_block_time >= current_date - interval '14' day
        {%- endif %}
),

candidates as (
    select id, block_time, block_number, evt_index, block_index_sum, sqrtpricex96
    from aged_events
    {%- if is_incremental() %}

    union all

    select id, block_time, block_number, evt_index, block_index_sum, sqrtpricex96
    from {{ this }}
    {%- endif %}
)

select
    '{{ blockchain }}' as blockchain
    , id
    , max_by(block_time, block_index_sum) as block_time
    , max_by(block_number, block_index_sum) as block_number
    , max_by(evt_index, block_index_sum) as evt_index
    , max(block_index_sum) as block_index_sum
    , max_by(sqrtpricex96, block_index_sum) as sqrtpricex96
from candidates
where block_index_sum is not null
group by id

{% endmacro %}
