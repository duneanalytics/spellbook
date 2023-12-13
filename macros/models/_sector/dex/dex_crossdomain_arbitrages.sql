{% macro dex_crossdomain_arbitrages(blockchain, blocks, traces, transactions, erc20_transfers, dex_sandwiches) %}

WITH top_of_block AS (
    SELECT block_number
    , approx_percentile(index, 0.05) AS top_of_block_index_limit
    , approx_percentile(priority_fee_per_gas, 0.95) AS top_of_block_priority_fee
    , approx_percentile(priority_fee_per_gas, 0.5) AS median_priority_fee
    , stddev(priority_fee_per_gas) AS stddev_priority_fee
    FROM {{transactions}}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
    GROUP BY 1
    ORDER BY 2
    )

, paid_builder AS (
    -- List who tipped the block builder with native token
    SELECT dt.block_time
    , dt.tx_hash
    , dt.evt_index
    , txs.index AS tx_index
    , txs.block_number
    FROM {{ ref('dex_trades')}} dt
    INNER JOIN {{blocks}} b ON b.time=dt.block_time
        {% if is_incremental() %}
        AND {{ incremental_predicate('b.time') }}
        {% endif %}
    INNER JOIN {{transactions}} txs ON txs.block_time=dt.block_time
        AND txs.hash=dt.tx_hash
        {% if is_incremental() %}
        AND {{ incremental_predicate('txs.block_time') }}
        {% endif %}
    INNER JOIN {{traces}} t ON t.block_time=dt.block_time
        AND t.success
        AND (t.call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR t.call_type IS NULL)
        AND t.value > 0
        AND t.to=b.miner
        --AND t."from"!=0x0000000000000000000000000000000000000000
        AND t."from"=dt.tx_from
        --AND txs.index IN (t.tx_index-1, t.tx_index, t.tx_index+1)
        {% if is_incremental() %}
        AND {{ incremental_predicate('t.block_time') }}
        {% endif %}
    WHERE dt.blockchain = '{{blockchain}}'
    {% if is_incremental() %}
    AND {{ incremental_predicate('dt.block_time') }}
    {% endif %}
    
    UNION ALL
    
    SELECT dt.block_time
    , dt.tx_hash
    , dt.evt_index
    , txs.index AS tx_index
    , txs.block_number
    FROM {{ ref('dex_trades')}} dt
    INNER JOIN {{blocks}} b ON b.time=dt.block_time
    INNER JOIN {{transactions}} txs ON txs.block_time=dt.block_time
        AND txs.hash=dt.tx_hash
        {% if is_incremental() %}
        AND {{ incremental_predicate('txs.block_time') }}
        {% endif %}
    INNER JOIN {{erc20_transfers}} t ON t.evt_block_time=dt.block_time
        AND t.value > 0
        AND t.to=b.miner
        {% if is_incremental() %}
        AND {{ incremental_predicate('t.evt_block_time') }}
        {% endif %}
    INNER JOIN {{transactions}} txs2 ON txs2.block_time=t.evt_block_time
        --AND txs2.index IN (txs.index-1, txs.index, txs.index+1)
        AND txs2."from"=dt.tx_from
        {% if is_incremental() %}
        AND {{ incremental_predicate('txs2.block_time') }}
        {% endif %}
    WHERE dt.blockchain = '{{blockchain}}'
    {% if is_incremental() %}
    AND {{ incremental_predicate('dt.block_time') }}
    {% endif %}
    )

, single_swap_txs AS (
    SELECT DISTINCT block_number, tx_index, block_time, tx_hash, evt_index
    FROM paid_builder t1
    WHERE NOT EXISTS (
        SELECT 1
        FROM paid_builder t2
        WHERE t1.block_number = t2.block_number
        AND t1.tx_index = t2.tx_index
        AND t1.evt_index != t2.evt_index
        )
    )

SELECT i.block_time
, i.block_number
, i.tx_hash
, i.evt_index
, dt.blockchain
, dt.project
, dt.version
, dt.block_month
, dt.token_sold_symbol
, dt.token_bought_symbol
, dt.token_sold_address
, dt.token_bought_address
, dt.token_pair
, dt.token_sold_amount
, dt.token_bought_amount
, dt.token_sold_amount_raw
, dt.token_bought_amount_raw
, dt.amount_usd
, dt.taker
, dt.maker
, dt.project_contract_address
, dt.tx_from
, dt.tx_to
, i.tx_index
FROM single_swap_txs i
INNER JOIN {{ ref('dex_trades')}} dt ON dt.blockchain = '{{blockchain}}'
    AND dt.block_time=i.block_time
    AND dt.tx_hash=i.tx_hash
    AND dt.evt_index=i.evt_index
    {% if is_incremental() %}
    AND {{ incremental_predicate('dt.block_time') }}
    {% endif %}
LEFT JOIN {{dex_sandwiches}} ds ON i.block_time=ds.block_time
    AND i.tx_hash=ds.tx_hash
    AND i.evt_index=ds.evt_index
    AND ds.evt_index IS NULL
    {% if is_incremental() %}
    AND {{ incremental_predicate('ds.block_time') }}
    {% endif %}

{% endmacro %}