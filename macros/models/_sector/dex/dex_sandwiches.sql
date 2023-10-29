{% macro dex_sandwiches(blockchain, transactions) %}

-- CTE no longer needed whenever dex.trades will have block_number & tx_index
WITH indexed_trades AS (
    SELECT dt.block_time
    , tx.block_number
    , dt.project_contract_address
    , dt.tx_from
    , tx.index AS tx_index
    , dt.token_sold_address
    , dt.token_bought_address
    , dt.tx_hash
    , dt.evt_index
    FROM {{ ref('dex_trades') }} dt
    INNER JOIN {{transactions}} tx ON tx.block_time=dt.block_time
        AND tx.hash=dt.tx_hash
        {% if is_incremental() %}
        AND tx.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    WHERE dt.blockchain='ethereum'
    {% if is_incremental() %}
    AND dt.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    )

-- Checking that frontrun trade (s1) has a matching backrun (s2) and at least one victim in between
, sandwiches AS (
    SELECT DISTINCT s1.block_time
    , s1.block_number
    , s1.project_contract_address
    , s1.tx_index
    , evt_index_all AS evt_index
    FROM indexed_trades s1
    INNER JOIN indexed_trades s2 ON s1.block_number=s2.block_number
        AND s1.project_contract_address=s2.project_contract_address
        AND s1.tx_from=s2.tx_from
        AND s1.tx_index<s2.tx_index-1
        AND s1.token_sold_address=s2.token_bought_address
        AND s1.token_bought_address=s2.token_sold_address
    INNER JOIN indexed_trades victim ON s1.block_number=victim.block_number
        AND s1.project_contract_address=victim.project_contract_address
        AND victim.tx_index BETWEEN s1.tx_index AND s2.tx_index
        AND victim.tx_from!=s1.tx_from
        AND s1.token_bought_address=victim.token_bought_address
        AND s1.token_sold_address=victim.token_sold_address
    CROSS JOIN UNNEST(ARRAY[s1.evt_index, s2.evt_index]) AS t(evt_index_all)
    )

-- Joining back with dex.trades to get the rest of the data
SELECT dt.blockchain
, dt.project
, dt.version
, block_time
, CAST(date_trunc('month', block_time) AS date) AS block_month
, s.block_number
, dt.token_sold_address
, dt.token_bought_address
, dt.token_sold_symbol
, dt.token_bought_symbol
, dt.maker
, dt.taker
, dt.tx_hash
, dt.tx_from
, dt.tx_to
, project_contract_address
, dt.token_pair
, s.tx_index
, dt.token_sold_amount_raw
, dt.token_bought_amount_raw
, dt.token_sold_amount
, dt.token_bought_amount
, dt.amount_usd
, evt_index
FROM {{ ref('dex_trades') }} dt
INNER JOIN sandwiches s USING (block_time, project_contract_address, evt_index)

{% endmacro %}