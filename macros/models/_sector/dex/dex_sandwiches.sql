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
, tx1."from" AS tx_from
, tx1.to AS tx_to
, s1.project_contract_address
, s1.token_pair
, tx1.index
, s1.token_sold_amount_raw
, s1.token_bought_amount_raw
, s1.token_sold_amount
, s1.token_bought_amount
, CASE WHEN tx1."from"=tx2."from" THEN 'tx_from' ELSE 'taker' END AS commonality
, CASE WHEN s1.token_bought_address=s2.token_sold_address THEN 'token_sold' ELSE 'token_bought' END AS sandwiched_token
, CASE WHEN s1.token_bought_address<s1.token_sold_address THEN 0 ELSE 1 END AS token_order
FROM trades_with_index s1
INNER JOIN trades_with_index s2 ON s1.block_number=s2.block_number
    AND s1.project=s2.project
    AND s1.version=s2.version
    AND s1.tx_hash!=s2.tx_hash
    AND s1.project_contract_address=s2.project_contract_address
    --AND s2.token_sold_amount BETWEEN s1.token_bought_amount*0.9 AND s1.token_bought_amount*1.1
INNER JOIN {{transactions}} tx1 ON  dt.blockchain='ethereum'
    AND tx1.block_time=dt.block_time
    AND tx1.hash=s1.tx_hash
    AND tx1.block_time >= date_trunc('day', now() - interval '3' day)
    {% if is_incremental() %}
    AND tx1.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
INNER JOIN {{transactions}} tx2 ON  dt.blockchain='ethereum'
    AND tx2.block_time=dt.block_time
    AND tx2.hash=s2.tx_hash
    AND (tx1."from"=tx2."from" OR s1.taker=s2.taker)
    AND ((tx1.index>tx2.index AND s1.token_bought_address=s2.token_sold_address)
        OR (tx1.index<tx2.index AND s1.token_sold_address=s2.token_bought_address))
    AND tx2.block_time >= date_trunc('day', now() - interval '3' day)
    {% if is_incremental() %}
    AND tx2.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

{% endmacro %}