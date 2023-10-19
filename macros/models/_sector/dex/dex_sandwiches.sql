{% macro dex_sandwiches(blockchain, transactions) %}

-- Summing up the amounts by traded pool+token pair and issuing distinct id per swap
-- , this is because some tokens have swap limits which can be circumvented by using multiple swaps, this regroups those
WITH sequenced_trades AS (
    SELECT block_time
    , tx_hash
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
    GROUP BY 1, 2, 3, 4, 6
    )
    
-- Joining each swap with others in same tx where token_sold_address = s2.token_bought_address
, paired_trades AS (
    SELECT DISTINCT s1.block_time
    , s1.tx_hash
    , s1.project_contract_address AS project_contract_address_1
    , s2.project_contract_address AS project_contract_address_2
    , s1.seq_num AS seq_num_1
    , s2.seq_num AS seq_num_2
    , CASE WHEN s1.token_sold_amount <= s2.token_bought_amount THEN true ELSE false END AS first_is_less
    , s1.project_evt_pairs AS project_evt_pairs_1
    , s2.project_evt_pairs AS project_evt_pairs_2
    FROM sequenced_trades s1
    INNER JOIN sequenced_trades s2 ON s1.tx_hash = s2.tx_hash
        AND s1.token_sold_address = s2.token_bought_address
        AND s1.seq_num != s2.seq_num
    )

-- If it's an arb trade, the swaps should loop and thus when grouping by tx_hash, seq_num_1s has to be equal to seq_num_2s
, arbitrage_trades AS (
    SELECT distinct block_time
    , tx_hash
    , array_distinct(flatten(array_agg(project_evt_pairs_1) || array_agg(project_evt_pairs_2))) AS project_evt_pairs
    --, array_agg() AS evt_indices
    --, COUNT(*) FILTER (WHERE first_is_less = TRUE) AS first_is_less_count
    --, COUNT(*) FILTER (WHERE first_is_less = FALSE) AS first_is_more_count
    --, array_agg(DISTINCT seq_num_1 ORDER BY seq_num_1) AS seq_num_1s
    --, array_agg(DISTINCT seq_num_2 ORDER BY seq_num_2) AS seq_num_2s
    FROM paired_trades
    GROUP BY 1, 2
    HAVING array_agg(DISTINCT seq_num_1 ORDER BY seq_num_1)=array_agg(DISTINCT seq_num_2 ORDER BY seq_num_2)
    AND (COUNT(*) FILTER (WHERE first_is_less = FALSE)) > 0
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
INNER JOIN {{ ref('dex_trades') }} dt ON arb.block_time=dt.block_time
    AND arb.tx_hash=dt.tx_hash
    AND t.project_contract_address=dt.project_contract_address
    AND t.evt_index=dt.evt_index
INNER JOIN {{transactions}} txs ON arb.block_time=txs.block_time
    AND arb.tx_hash=txs.hash
    {% if is_incremental() %}
    AND txs.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

{% endmacro %}