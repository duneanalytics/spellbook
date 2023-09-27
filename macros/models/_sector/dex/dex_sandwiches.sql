{% macro dex_sandwiches(blockchain, transactions) %}

SELECT distinct s1.blockchain
, s1.project
, s1.version
, s1.block_time
, CAST(date_trunc('month', s1.block_time) AS date) AS block_month
, tx1.block_number
, s1.token_sold_address
, s1.token_bought_address
, s1.token_sold_symbol
, s1.token_bought_symbol
, s1.maker
, s1.taker
, s1.tx_hash
, s1.tx_from
, s1.tx_to
, s1.project_contract_address
, s1.token_pair
, tx1.index
, s1.token_sold_amount_raw
, s1.token_bought_amount_raw
, s1.token_sold_amount
, s1.token_bought_amount
, s1.evt_index
--, CASE WHEN s1.tx_from=s2.tx_from THEN 'tx_from' ELSE 'taker' END AS commonality
--, CASE WHEN s1.token_bought_address=s2.token_sold_address THEN 'token_sold' ELSE 'token_bought' END AS sandwiched_token
--, CASE WHEN s1.token_bought_address<s1.token_sold_address THEN 0 ELSE 1 END AS token_order
FROM {{ ref('dex_trades') }} s1
INNER JOIN {{ ref('dex_trades') }} s2 ON s1.blockchain='{{blockchain}}'
    AND s2.blockchain='{{blockchain}}'
    AND s1.block_time=s2.block_time
    AND s1.project=s2.project
    AND s1.version=s2.version
    AND s1.tx_hash!=s2.tx_hash
    AND s1.project_contract_address=s2.project_contract_address
    {% if is_incremental() %}
    AND s1.block_time >= date_trunc('day', now() - interval '7' day)
    AND s2.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
INNER JOIN {{transactions}} tx1 ON tx1.block_time=s1.block_time
    AND tx1.hash=s1.tx_hash
    {% if is_incremental() %}
    AND tx1.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
INNER JOIN {{transactions}} tx2 ON tx2.block_time=s2.block_time
    AND tx2.hash=s2.tx_hash
    AND (s1.tx_from=s2.tx_from OR s1.taker=s2.taker)
    AND ((tx1.index>tx2.index AND s1.token_bought_address=s2.token_sold_address)
        OR (tx1.index<tx2.index AND s1.token_sold_address=s2.token_bought_address))
    {% if is_incremental() %}
    AND tx2.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

{% endmacro %}