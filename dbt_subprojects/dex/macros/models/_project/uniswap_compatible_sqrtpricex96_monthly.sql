{#
    Sparse monthly-latest rollup companion for uniswap_compatible_v4_liquidity_sqrtpricex96.

    Emits, per (pool id, calendar month), the latest sqrtPriceX96 price-change event in
    that month (max_by over block_index_sum). One row per pool per ACTIVE month -- sparse
    and event-grain, NOT forward-filled: a pool appears only for months in which it had at
    least one price event.

    The price builder reads this instead of self-scanning all of its own history to recover
    each active pool's previous price. For an incremental window it seeds each active pool
    from the latest month strictly before the window (this rollup) unioned with the current
    partial month (a bounded self-read). See uniswap_compatible_v4_liquidity_sqrtpricex96.

    Idempotency: the merge key is (blockchain, id, block_month) and the incremental reload
    window is the most-recent slice of time, so a pool's window-latest event IS its
    month-latest event. A forward-moving window converges each month's row to that month's
    true latest and then leaves it frozen, so arbitrary-window / adhoc builds are
    reproducible -- unlike a now()-anchored full-history "latest per pool" snapshot, which
    silently drops seeds for pools last active before the reload window.

    Built from the raw event sources (not from the price builder's own output) so the two
    models do not form a dbt ref cycle.
#}
{% macro uniswap_compatible_v4_sqrtpricex96_monthly(
    blockchain = null
    , project = 'uniswap'
    , version = '4'
    , PoolManager_evt_Initialize = null
    , PoolManager_evt_Swap = null
    , transactions = null
    )
%}

with

month_events as (
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
        {%- if is_incremental() %}
        and {{ incremental_predicate('tx.block_time') }}
        {%- endif %}
    where e.sqrtPriceX96 is not null
        {%- if is_incremental() %}
        and {{ incremental_predicate('e.evt_block_time') }}
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
        {%- if is_incremental() %}
        and {{ incremental_predicate('tx.block_time') }}
        {%- endif %}
    where e.sqrtPriceX96 is not null
        {%- if is_incremental() %}
        and {{ incremental_predicate('e.evt_block_time') }}
        {%- elif target.name == 'ci' %}
        and e.evt_block_time >= current_date - interval '14' day
        {%- endif %}
)

select
    '{{ blockchain }}' as blockchain
    , id
    , cast(date_trunc('month', block_time) as date) as block_month
    , max_by(block_time, block_index_sum) as block_time
    , max_by(block_number, block_index_sum) as block_number
    , max_by(evt_index, block_index_sum) as evt_index
    , max(block_index_sum) as block_index_sum
    , max_by(sqrtpricex96, block_index_sum) as sqrtpricex96
from month_events
where block_index_sum is not null
group by id, cast(date_trunc('month', block_time) as date)

{% endmacro %}
