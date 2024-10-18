{% macro tokens_info(blockchain) %}

WITH transfers AS (
    SELECT contract_address AS address
    , symbol AS token_symbol
    , token_standard
    , MIN(block_time) AS first_block_time
    , MIN(block_number) AS first_block_number
    , MAX(block_time) AS last_block_time
    , MAX(block_number) AS last_block_number
    , COUNT(DISTINCT tx_hash) AS transactions
    , COUNT(*) AS transfers
    FROM {{ source('tokens_'~blockchain, 'transfers')}}
    GROUP BY 1, 2, 3
    LIMIT 1000000
    )

, dexs AS (
    SELECT token_sold_address AS address
    , SUM(trades) AS trades
    , SUM(volume) AS volume
    , array_distinct(array_union_agg(found_on_dexs)) AS found_on_dexs
    FROM (
        SELECT token_sold_address AS address
        , COUNT(*) AS trades
        , SUM(amount_usd) AS volume
        , array_distinct(array_agg(project)) AS found_on_dexs
        FROM {{ source('dex', 'trades')}}
        WHERE blockchain = '{{blockchain}}'
        GROUP BY 1

        UNION ALL

        SELECT token_bought_address AS address
        , COUNT(*) AS trades
        , SUM(amount_usd) AS volume
        , array_distinct(array_agg(project)) AS found_on_dexs
        FROM {{ source('dex', 'trades')}}
        WHERE blockchain = '{{blockchain}}'
        GROUP BY 1
        )
    GROUP BY 1
    )

SELECT address AS token_address
, t.token_symbol
, ct."from" AS creator
, i.namespace
, i.name
, t.token_standard
, trades
, d.volume
, d.found_on_dexs
, t.transactions
, t.transfers
, t.first_block_time
, t.first_block_number
, t.last_block_time
, t.last_block_number
FROM transfers t
INNER JOIN {{ ref('addresses_'~blockchain~'_info')}} i USING (address)
INNER JOIN {{ source(blockchain, 'creation_traces')}} ct USING (address)
LEFT JOIN dexs d USING (address)
LIMIT 100000

{% endmacro %}
