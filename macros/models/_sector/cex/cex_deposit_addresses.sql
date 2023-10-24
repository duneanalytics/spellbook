{% macro cex_deposit_addresses(blockchain, transactions, traces, erc20_transfers, cex_addresses) %}

WITH first_txs AS (
    SELECT block_time
    , block_number
    , hash AS tx_hash
    , "from" AS tx_from
    FROM {{ transactions }}
    WHERE nonce=0
    {% if is_incremental() %}
    AND block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    )

, commingle_transactions AS (
    SELECT txs.block_time
    , txs.block_number
    , "from" AS deposit_address
    , to AS cex_address
    , cex.cex_name
    , cex.distinct_name
    , txs.tx_hash
    , 'native' AS deposit_token_type
    FROM first_txs txs
    INNER JOIN {{ traces }} traces ON txs.block_number=traces.block_number
        AND txs.tx_hash=traces.tx_hash
        AND (traces.call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR traces.call_type IS NULL)
        AND traces.success
        {% if is_incremental() %}
        AND traces.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    INNER JOIN {{ cex_addresses }} cex ON cex.address=traces.to
    
    UNION ALL
    
    SELECT t.evt_block_time AS block_time
    , t.evt_block_number AS block_number
    , t."from" AS deposit_address
    , t.to AS cex_address
    , cex.cex_name
    , cex.distinct_name
    , t.evt_tx_hash AS tx_hash
    , 'erc20' AS deposit_token_type
    FROM first_txs txs
    INNER JOIN {{ erc20_transfers }} t ON txs.block_number=t.evt_block_number
        AND txs.tx_hash=t.evt_tx_hash
        {% if is_incremental() %}
        AND t.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    INNER JOIN {{ erc20_transfers }} t2 ON txs.block_number=t2.evt_block_number
        AND txs.tx_hash=t2.evt_tx_hash
        AND t."from"=t2.to
        AND t2.evt_block_time BETWEEN t.evt_block_time - interval '1' day AND t.evt_block_time
        {% if is_incremental() %}
        AND t2.evt_block_time >= date_trunc('day', now() - interval '8' day)
        {% endif %}
    INNER JOIN {{ cex_addresses }} cex ON cex.address=t.to
    )

, distinct_commingle_transactions AS (
    SELECT block_time
    , block_number
    ,  deposit_address
    , cex_address
    , cex_name
    , distinct_name
    , tx_hash
    , MIN(deposit_token_type) AS deposit_token_type
    FROM commingle_transactions
    GROUP BY 1, 2, 3, 4, 5, 6, 7
    )

SELECT '{{blockchain}}' AS blockchain
, date_trunc('month', dct.block_time) AS block_month
, dct.block_time
, dct.block_number
, dct.deposit_address
, dct.cex_address
, dct.cex_name
, dct.distinct_name
, dct.tx_hash
, dct.deposit_token_type
, array_agg(traces."from") AS eth_funders
FROM distinct_commingle_transactions dct
INNER JOIN {{ traces }} traces ON dct.block_number=traces.block_number
    AND dct.deposit_address=traces.to
    AND (traces.call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR traces.call_type IS NULL)
    AND traces.success
    AND traces.block_time BETWEEN dct.block_time - interval '1' day AND dct.block_time
    {% if is_incremental() %}
    AND traces.block_time >= date_trunc('day', now() - interval '8' day)
    {% endif %}
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

{% endmacro %}