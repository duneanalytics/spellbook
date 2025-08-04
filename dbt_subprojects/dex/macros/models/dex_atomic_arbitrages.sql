{% macro dex_atomic_arbitrages(blockchain, transactions) %}

-- Step 1: Check that the transaction contains at least 2 trades
WITH multi_trade_txs AS (
    SELECT block_time, tx_hash
    FROM {{ ref('dex_trades') }}
    WHERE blockchain = '{{blockchain}}'
    {% if is_incremental() %}
    AND {{ incremental_predicate('block_time') }}
    {% endif %}
    GROUP BY 1, 2
    HAVING COUNT(*) > 1
    )

-- Step 2: Fetch more columns from the trades table
, filled_multi_trade_txs AS (
    SELECT block_time
    , tx_hash
    , dt.token_sold_address
    , dt.token_bought_address
    , dt.token_sold_amount_raw
    , dt.token_bought_amount_raw
    FROM {{ ref('dex_trades') }} dt
    INNER JOIN multi_trade_txs pa USING (block_time, tx_hash)
    WHERE dt.blockchain = '{{blockchain}}'
    {% if is_incremental() %}
    AND {{ incremental_predicate('block_time') }}
    {% endif %}
    )

-- Step 3: Unnest all traded tokens and filter out negative sum balances
, only_positive_balance_tokens AS (
    SELECT block_time
    , tx_hash
    , token_address
    FROM filled_multi_trade_txs
    CROSS JOIN UNNEST(ARRAY[(token_bought_address, token_bought_amount_raw), (token_sold_address, -token_sold_amount_raw)]) AS t(token_address, token_amount_raw)
    GROUP BY 1, 2, 3
    HAVING SUM(token_amount_raw) >= 0
    )

-- Step 4: Group all valid tokens per transaction
, whitelisted_tokens AS (
    SELECT block_time
    , tx_hash
    , array_agg(token_address) AS token_addresses
    FROM only_positive_balance_tokens
    GROUP BY 1, 2
    )

-- Step 5: Filter out trades containing negative sum tokens from txs and add block_number and tx_index
, positive_sum_trades AS (
    SELECT dt.block_time
    , txs.block_number
    , dt.tx_hash
    , dt.evt_index
    , dt.blockchain
    , dt.project
    , dt.version
    , dt.block_date
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
    , txs.index AS tx_index
    FROM {{ ref('dex_trades') }} dt
    INNER JOIN whitelisted_tokens wt ON dt.block_time=wt.block_time
        AND dt.tx_hash=wt.tx_hash
        AND CONTAINS(wt.token_addresses, dt.token_sold_address)
        AND CONTAINS(wt.token_addresses, dt.token_bought_address)
    INNER JOIN {{transactions}} txs ON dt.block_time=txs.block_time
        AND dt.tx_hash=txs.hash
        {% if is_incremental() %}
        AND {{ incremental_predicate('txs.block_time') }}
        {% endif %}
    WHERE dt.blockchain = '{{blockchain}}'
    {% if is_incremental() %}
    AND {{ incremental_predicate('dt.block_time') }}
    {% endif %}
    )

-- Step 6: Only keep trades that form a loop
SELECT block_time
, block_number
, tx_hash
, evt_index
, blockchain
, project
, version
, block_month
, token_sold_symbol
, token_bought_symbol
, token_sold_address
, token_bought_address
, token_pair
, token_sold_amount
, token_bought_amount
, token_sold_amount_raw
, token_bought_amount_raw
, amount_usd
, taker
, maker
, project_contract_address
, tx_from
, tx_to
, tx_index
FROM positive_sum_trades
MATCH_RECOGNIZE (
    PARTITION BY block_number, tx_hash
    ORDER BY evt_index
    ALL ROWS PER MATCH
    PATTERN (A B+ D+)
    DEFINE
        A AS TRUE 
        , B AS token_sold_address = PREV(token_bought_address)
        , D AS FIRST(token_sold_address) = LAST(token_bought_address)
    ) pst

{% endmacro %}
