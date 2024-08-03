{% macro addresses_info(blockchain, transactions, token_transfers, creation_traces, first_funded_by, contracts) %}

{% if not is_incremental() %}

WITH executed_txs AS (
    SELECT "from" AS address
    , COUNT(*) AS executed_tx_count
    , COALESCE(MAX(nonce), 0) AS max_nonce
    , MAX(block_time) AS last_tx_block_time
    , MAX(block_number) AS last_tx_block_number
    FROM {{transactions}}
    GROUP BY 1
    )

, is_contract AS (
    SELECT ct.address
    , true AS is_smart_contract
    , c.namespace
    , c.name
    FROM {{creation_traces}} ct
    LEFT JOIN {{contracts}} c ON ct.address=c.address
    )

SELECT '{{blockchain}}' AS blockchain
, address
, executed_tx_count
, max_nonce
, COALESCE(is_smart_contract, false) AS is_smart_contract
, namespace
, name
, last_tx_block_time AS last_seen
, last_tx_block_number
FROM executed_txs
LEFT JOIN is_contract USING (address)



{% else %}



WITH executed_txs AS (
    SELECT txs."from" AS address
    , COUNT(*) AS executed_tx_count
    , MAX(txs.nonce) AS max_nonce
    , MAX(txs.block_time) AS last_tx_block_time
    , MAX(txs.block_number) AS last_tx_block_number
    FROM {{transactions}} txs
    LEFT JOIN {{this}} t ON txs."from"=t.address
        AND txs.block_number>t.last_tx_block_number
    WHERE {{ incremental_predicate('txs.block_time') }}
    GROUP BY 1
    )

, is_contract AS (
    SELECT ct.address
    , true AS is_smart_contract
    , c.namespace
    , c.name
    FROM {{creation_traces}} ct
    LEFT JOIN {{this}} t ON ct.address=t.address
        AND ct.block_number>t.last_tx_block_number
    LEFT JOIN {{contracts}} c ON ct.address=c.address
    WHERE {{ incremental_predicate('ct.block_time') }}
    )

, new_data AS (
    SELECT address
    , executed_tx_count
    , max_nonce
    , is_smart_contract
    , namespace
    , name
    , last_tx_block_time
    , last_tx_block_number
    FROM executed_txs
    LEFT JOIN is_contract USING (address)
    )

SELECT '{{blockchain}}' AS blockchain
, nd.address
, nd.executed_tx_count + t.executed_tx_count AS executed_tx_count
, COALESCE(nd.max_nonce, t.max_nonce) AS max_nonce
, COALESCE(COALESCE(nd.is_smart_contract, t.is_smart_contract), false) AS is_smart_contract
, COALESCE(nd.namespace, t.namespace) AS namespace
, COALESCE(nd.name, t.name) AS name
, GREATEST(nd.last_tx_block_time, t.last_tx_block_time) AS last_seen
, GREATEST(nd.last_tx_block_number, t.last_tx_block_number) AS last_tx_block_number
FROM new_data nd
LEFT JOIN {{this}} t ON t.address=nd.address

{% endif %}

{% endmacro %}