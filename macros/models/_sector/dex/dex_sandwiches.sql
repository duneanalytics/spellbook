{% macro dex_sandwiches(blockchain, transactions) %}

WITH trades AS (
    SELECT blockchain
    , project
    , version
    , block_time
    , token_sold_address
    , token_bought_address
    , token_sold_symbol
    , token_bought_symbol
    , maker
    , taker
    , tx_hash
    , tx_from
    , tx_to
    , project_contract_address
    , evt_index
    , token_pair
    , array_distinct(array_agg(evt_index ORDER BY evt_index)) AS evt_indices
    , SUM(COALESCE(token_sold_amount_raw, UINT256 '0')) AS token_sold_amount_raw
    , SUM(COALESCE(token_bought_amount_raw, UINT256 '0')) AS token_bought_amount_raw
    , SUM(COALESCE(token_sold_amount, 0)) AS token_sold_amount
    , SUM(COALESCE(token_bought_amount, 0)) AS token_bought_amount
    , SUM(COALESCE(amount_usd, 0)) AS amount_usd
    FROM {{ ref('dex_trades') }}
    WHERE blockchain='{{blockchain}}'
    {% if is_incremental() %}
    AND block_date >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
    )

, trades_with_index AS (
    SELECT dt.blockchain
    , dt.project
    , dt.version
    , dt.block_time
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
    , dt.evt_index
    , dt.evt_indices
    , dt.token_pair
    , dt.token_sold_amount_raw
    , dt.token_bought_amount_raw
    , dt.token_sold_amount
    , dt.token_bought_amount
    , dt.amount_usd
    , tx.block_number
    , tx.index
    FROM trades dt
    INNER JOIN {{transactions}} tx ON tx.block_time=dt.block_time
        AND tx.hash=dt.tx_hash
        {% if is_incremental() %}
        AND tx.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    )

SELECT distinct s1.blockchain
, s1.project
, s1.version
, s1.block_time
, date_trunc('month', s1.block_time) AS block_month
, s1.block_number
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
, s1.evt_indices
, s1.token_pair
, s1.index
, s1.token_sold_amount_raw
, s1.token_bought_amount_raw
, s1.token_sold_amount
, s1.token_bought_amount
FROM trades_with_index s1
INNER JOIN trades_with_index s2 ON s1.block_number=s2.block_number
    AND s1.project=s2.project
    AND s1.version=s2.version
    AND s1.tx_hash!=s2.tx_hash
    AND s1.project_contract_address=s2.project_contract_address
    AND (s1.tx_from=s2.tx_from OR s1.taker=s2.taker)
    AND ((s1.index>s2.index AND s1.token_bought_address=s2.token_sold_address)
        OR (s1.index<s2.index AND s1.token_sold_address=s2.token_bought_address))
    --AND s2.token_sold_amount BETWEEN s1.token_bought_amount*0.9 AND s1.token_bought_amount*1.1

{% endmacro %}