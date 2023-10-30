{% macro dex_sandwiches(blockchain, transactions) %}

-- Checking that each frontrun trade has a matching backrun and at least one victim in between
-- Joins for tx_f, tx_b & tx_v will no longer be needed when dex.trades has block_number & tx_index
WITH indexed_sandwich_trades AS (
    SELECT DISTINCT front.block_time
    , tx_f.block_number
    , front.project_contract_address
    , t.tx_hash_all AS tx_hash
    , t.index_all AS tx_index
    , t.evt_index_all AS evt_index
    FROM {{ ref('dex_trades') }} front
    INNER JOIN {{ ref('dex_trades') }} back ON back.blockchain='{{blockchain}}'
        AND front.block_time=back.block_time
        AND front.project_contract_address=back.project_contract_address
        AND front.tx_from=back.tx_from
        AND front.tx_hash!=back.tx_hash
        AND front.token_sold_address=back.token_bought_address
        AND front.token_bought_address=back.token_sold_address
        {% if is_incremental() %}
        AND back.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    INNER JOIN {{transactions}} tx_f ON tx_f.block_time=front.block_time
        AND tx_f.hash=front.tx_hash
        {% if is_incremental() %}
        AND tx_f.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    INNER JOIN {{transactions}} tx_b ON tx_b.block_time=back.block_time
        AND tx_b.hash=back.tx_hash
        AND tx_f.index + 1 < tx_b.index
        {% if is_incremental() %}
        AND tx_b.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    INNER JOIN {{ ref('dex_trades') }} victim ON victim.blockchain='{{blockchain}}'
        AND front.block_time=victim.block_time
        AND front.project_contract_address=victim.project_contract_address
        AND victim.tx_from!=front.tx_from
        AND front.token_bought_address=victim.token_bought_address
        AND front.token_sold_address=victim.token_sold_address
        {% if is_incremental() %}
        AND victim.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    INNER JOIN {{transactions}} tx_v ON tx_v.block_time=front.block_time
        AND tx_v.index BETWEEN tx_f.index AND tx_b.index
        {% if is_incremental() %}
        AND tx_v.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    CROSS JOIN UNNEST(ARRAY[(front.tx_hash, tx_f.index, front.evt_index), (back.tx_hash, tx_b.index, back.evt_index)]) AS t(tx_hash_all, evt_index_all, index_all)
    WHERE front.blockchain='{{blockchain}}'
    {% if is_incremental() %}
    AND front.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    )

-- Joining back with dex.trades to get the rest of the data
SELECT dt.blockchain
, dt.project
, dt.version
, block_time
, dt.block_month
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
INNER JOIN indexed_sandwich_trades s USING (block_time, tx_hash, project_contract_address, evt_index)

{% endmacro %}