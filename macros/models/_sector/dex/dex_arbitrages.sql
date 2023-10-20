{% macro dex_arbitrages(blockchain, transactions, fungible_transfers) %}

-- Summing up the amounts by traded pool+token pair and issuing distinct id per swap
-- , this is because some tokens have swap limits which can be circumvented by using multiple swaps, this regroups those
WITH sequenced_trades AS (
    SELECT block_time
    , tx_hash
    , taker
    , project_contract_address
    , token_bought_address
    , SUM(token_bought_amount) AS token_bought_amount
    , token_sold_address
    , SUM(token_sold_amount) AS token_sold_amount
    , ARRAY_AGG(ROW(project_contract_address, evt_index)) AS project_evt_pairs
    , ROW_NUMBER() OVER (PARTITION BY block_time, tx_hash) AS seq_num
    FROM {{ ref('dex_trades') }}
    WHERE blockchain='{{blockchain}}'
    {% if is_incremental() %}
    AND block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    GROUP BY 1, 2, 3, 4, 5, 7
    )
    
-- Joining each swap with others in same tx where token_sold_address = s2.token_bought_address
, paired_trades AS (
    SELECT DISTINCT s1.block_time
    , s1.tx_hash
    , s1.taker
    , CASE WHEN s1.token_sold_amount <= s2.token_bought_amount THEN true ELSE false END AS first_is_less
    , s1.project_evt_pairs AS project_evt_pairs_1
    , s2.project_evt_pairs AS project_evt_pairs_2
    , s1.token_sold_address AS token_sold_s1
    , s1.token_bought_address AS token_bought_s1
    , s2.token_sold_address AS token_sold_s2
    , s2.token_bought_address AS token_bought_s2
    FROM sequenced_trades s1
    INNER JOIN sequenced_trades s2 ON s1.tx_hash = s2.tx_hash
        AND s1.token_sold_address = s2.token_bought_address
        AND s1.seq_num != s2.seq_num
    )

, token_mappings AS (
    SELECT block_time
    , tx_hash
    , taker
    , array_distinct(array_agg(token_bought_s1) || array_agg(token_bought_s2)) AS tokens_bought
    , array_distinct(array_agg(token_sold_s1) || array_agg(token_sold_s2)) AS tokens_sold
    FROM paired_trades
    GROUP BY 1, 2, 3
    )

-- If it's an arb trade, the swaps should loop and thus when grouping by tx_hash, seq_num_1s has to be equal to seq_num_2s
, arbitrage_trades AS (
    SELECT distinct pt.block_time
    , pt.tx_hash
    , pt.taker
    , array_distinct(flatten(array_agg(pt.project_evt_pairs_1) || array_agg(pt.project_evt_pairs_2))) AS project_evt_pairs
    , COUNT(*) FILTER (WHERE first_is_less = TRUE) AS first_is_less
    , COUNT(*) FILTER (WHERE first_is_less = FALSE) AS first_is_more
    FROM paired_trades pt
    JOIN token_mappings tm ON pt.block_time=tm.block_time AND pt.tx_hash = tm.tx_hash
    INNER JOIN {{ fungible_transfers }} f ON pt.block_time=f.block_time
        AND pt.tx_hash=f.tx_hash
        AND pt.taker=f."from"
        AND f.amount_raw > UINT256 '0'
        -- No need to check token_sold_s1 & token_bought_s2 since those were joined on:
        AND (contains(tm.tokens_sold, pt.token_bought_s1) OR contains(array[f.contract_address], pt.token_bought_s1))
        AND (contains(tm.tokens_sold, pt.token_sold_s2) OR contains(array[f.contract_address], pt.token_sold_s2))
        {% if is_incremental() %}
        AND f.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    GROUP BY 1, 2, 3
    HAVING (COUNT(*) FILTER (WHERE first_is_less = FALSE)) > 0
    AND (COUNT(*) FILTER (WHERE first_is_less = TRUE)) > 0
    )

SELECT '{{blockchain}}' AS blockchain
, dt.project
, dt.version
, arb.block_time
, CAST(date_trunc('month', arb.block_time) AS date) AS block_month
, txs.block_number
, dt.token_sold_address
, dt.token_bought_address
, dt.token_sold_symbol
, dt.token_bought_symbol
, dt.maker
, dt.taker
, arb.tx_hash
, dt.tx_from
, dt.tx_to
, t.project_contract_address
, dt.token_pair
, txs.index
, dt.token_sold_amount_raw
, dt.token_bought_amount_raw
, dt.token_sold_amount
, dt.token_bought_amount
, dt.amount_usd
, t.evt_index
FROM arbitrage_trades arb
CROSS JOIN UNNEST(project_evt_pairs) AS t(project_contract_address, evt_index)
INNER JOIN {{ ref('dex_trades') }} dt ON dt.blockchain='{{blockchain}}'
    AND arb.block_time=dt.block_time
    AND arb.tx_hash=dt.tx_hash
    AND t.project_contract_address=dt.project_contract_address
    AND t.evt_index=dt.evt_index
    {% if is_incremental() %}
    AND dt.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
INNER JOIN {{transactions}} txs ON arb.block_time=txs.block_time
    AND arb.tx_hash=txs.hash
    {% if is_incremental() %}
    AND txs.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

{% endmacro %}