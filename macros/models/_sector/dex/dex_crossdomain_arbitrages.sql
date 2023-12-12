{% macro dex_crossdomain_arbitrages(blockchain, blocks, traces, transactions, erc20_transfers) %}

WITH top_of_block AS (
    SELECT block_number
    , approx_percentile(index, 0.9) AS top_of_block_index_limit
    FROM {{transactions}}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
    GROUP BY 1
    )

, paid_builder AS (
    -- List who tipped the block builder with native token
    SELECT b.time AS block_time
    , t."from" AS tx_from
    , t.tx_hash
    FROM {{traces}} t
    INNER JOIN {{blocks}} b ON b.number=t.block_number
        AND t.to=b.miner
        AND t."from"!=0x0000000000000000000000000000000000000000
        {% if is_incremental() %}
        AND {{ incremental_predicate('b.block_time') }}
        {% endif %}
    INNER JOIN {{transactions}} txs ON txs.block_number=t.block_number 
        AND txs.hash=t.tx_hash 
        AND txs."from"=t."from"
        {% if is_incremental() %}
        AND {{ incremental_predicate('txs.block_time') }}
        {% endif %}
    WHERE t.success
    AND (t.call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR t.call_type IS NULL)
    {% if is_incremental() %}
    AND {{ incremental_predicate('b.block_time') }}
    {% endif %}
    
    UNION ALL
    
    -- List who tipped the block builder with ER20 tokens
    SELECT b.time AS block_time
    , t."from" AS tx_from
    , t.evt_tx_hash AS tx_hash
    FROM {{erc20_transfers}} t
    INNER JOIN {{blocks}} b ON b.number=t.evt_block_number
        AND t.to=b.miner
        AND t."from"!=0x0000000000000000000000000000000000000000
        {% if is_incremental() %}
        AND {{ incremental_predicate('traces.block_time') }}
        {% endif %}
    INNER JOIN {{transactions}} txs ON txs.block_number=t.evt_block_number 
        AND txs.hash=t.evt_tx_hash 
        AND txs."from"=t."from"
        {% if is_incremental() %}
        AND {{ incremental_predicate('txs.block_time') }}
        {% endif %}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
    
    UNION ALL
    
    -- List all addresses behind transactions in the top of the block
    SELECT txs.block_time
    , txs."from" AS tx_from
    , txs.hash AS tx_hash
    FROM {{transactions}} txs
    INNER JOIN top_of_block tbl ON txs.block_number=tbl.block_number
        AND txs.index <= tbl.top_of_block_index_limit
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('txs.block_time') }}
    {% endif %}
    )

, distinct_transactions AS (
    SELECT DISTINCT pb.block_time
    , pb.tx_from
    , pb.tx_hash
    , txs.index AS tx_index
    FROM paid_builder pb
    INNER JOIN {{transactions}} txs ON pb.block_time=txs.block_time
        AND pb.tx_hash=txs.hash
        AND pb.tx_from=txs."from"
        {% if is_incremental() %}
        AND {{ incremental_predicate('txs.block_time') }}
        {% endif %}
    )

-- Exclusively keep trades where the builder was bribed
SELECT DISTINCT dt.blockchain
, dt.project
, dt.version
, dt.block_time
, dt.block_month
, txs.block_number
, dt.token_sold_address
, dt.token_bought_address
, dt.token_sold_symbol
, dt.token_bought_symbol
, dt.maker
, dt.taker
, dt.tx_hash
, dt.tx_from
, dt.tx_to
, dt.project_contract_address
, dt.token_pair
, txs.index AS tx_index
, dt.token_sold_amount_raw
, dt.token_bought_amount_raw
, dt.token_sold_amount
, dt.token_bought_amount
, dt.amount_usd
, dt.evt_index
FROM {{ ref('dex_trades')}} dt
INNER JOIN distinct_transactions i ON dt.block_time=i.block_time
    AND i.tx_from=dt.tx_from
INNER JOIN {{transactions}} txs ON txs.block_time=dt.block_time
    AND i.tx_index IN (txs.index-1, txs.index)
    {% if is_incremental() %}
    AND {{ incremental_predicate('txs.block_time') }}
    {% endif %}
WHERE dt.blockchain='{{blockchain}}'
{% if is_incremental() %}
AND {{ incremental_predicate('dt.block_time') }}
{% endif %}

{% endmacro %}