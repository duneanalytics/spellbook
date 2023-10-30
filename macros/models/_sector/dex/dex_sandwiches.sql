{% macro dex_sandwiches(blockchain, transactions) %}

-- Checking that each frontrun trade has a matching backrun and at least one victim in between
-- Joins for tx_f, tx_b & tx_v will no longer be needed when dex.trades has block_number & tx_index
WITH indexed_sandwich_trades AS (
    SELECT DISTINCT front.block_time
    , tx_f.block_number
    , front.project_contract_address
    , t.tx_index_all AS tx_index
    FROM {{ ref('dex_trades') }} front
    INNER JOIN {{ ref('dex_trades') }} back ON back.blockchain='{{blockchain}}'
        AND front.block_time=back.block_time
        AND front.project_contract_address=back.project_contract_address
        AND front.tx_from=back.tx_from
        AND front.tx_hash!=back.tx_hash
        AND front.token_sold_address=back.token_bought_address
        AND front.token_bought_address=back.token_sold_address
        AND front.evt_index + 1 < back.evt_index
        {% if is_incremental() %}
        AND back.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    INNER JOIN {{ ref('dex_trades') }} victim ON victim.blockchain='{{blockchain}}'
        AND front.block_time=victim.block_time
        AND front.project_contract_address=victim.project_contract_address
        AND victim.tx_from!=front.tx_from
        AND front.token_bought_address=victim.token_bought_address
        AND front.token_sold_address=victim.token_sold_address
        AND victim.evt_index BETWEEN front.evt_index AND back.evt_index
        {% if is_incremental() %}
        AND victim.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    CROSS JOIN UNNEST(ARRAY[front.evt_index, back.evt_index]) AS t(evt_index_all)
    WHERE front.blockchain='{{blockchain}}'
    {% if is_incremental() %}
    AND front.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    )

-- Joining back with dex.trades to get the rest of the data
SELECT dt.blockchain
, dt.project
, dt.version
, dt.block_time
, dt.block_month
, tx.block_number
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
, tx.index AS tx_index
, dt.token_sold_amount_raw
, dt.token_bought_amount_raw
, dt.token_sold_amount
, dt.token_bought_amount
, dt.amount_usd
, dt.evt_index
FROM {{ ref('dex_trades') }} dt
INNER JOIN indexed_sandwich_trades s ON dt.block_time=s.block_time
    AND dt.project_contract_address=s.project_contract_address
    AND dt.evt_index=s.tx_index
INNER JOIN {{transactions}} tx ON tx.block_time=dt.block_time
    AND tx.hash=dt.tx_hash
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
WHERE dt.blockchain='{{blockchain}}'
{% if is_incremental() %}
AND dt.block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}

{% endmacro %}