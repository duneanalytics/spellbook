{% macro addresses_info(blockchain, transactions, token_transfers, creation_traces, first_funded_by, contracts) %}

{% if not is_incremental() %}

WITH executed_txs AS (
    SELECT "from" AS address
    , COUNT(*) AS executed_tx_count
    , COALESCE(MAX(nonce), 0) AS max_nonce
    , MIN(block_time) AS first_tx_block_time
    , MAX(block_time) AS last_tx_block_time
    , MIN(block_number) AS first_tx_block_number
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
, first_funded_by
, first_tx_block_time
, last_tx_block_time
, first_tx_block_number
, last_tx_block_number
, last_tx_block_time AS last_seen
FROM executed_txs
LEFT JOIN is_contract USING (address)
LEFT JOIN {{ source('addresses_events_'~blockchain, 'first_funded_by')}} USING (address)



{% else %}



WITH executed_txs AS (
    SELECT txs."from" AS address
    , COUNT(*) AS executed_tx_count
    , MAX(txs.nonce) AS max_nonce
    , MIN(txs.block_time) AS first_tx_block_time
    , MAX(txs.block_time) AS last_tx_block_time
    , MIN(txs.block_number) AS first_tx_block_number
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
    , first_funded_by
    , first_tx_block_time
    , last_tx_block_time
    , first_tx_block_number
    , last_tx_block_number
    FROM executed_txs
    LEFT JOIN is_contract USING (address)
    LEFT JOIN {{ source('addresses_events_'~blockchain, 'first_funded_by')}} USING (address)
    )

SELECT '{{blockchain}}' AS blockchain
, nd.address
, nd.executed_tx_count + t.executed_tx_count AS executed_tx_count
, COALESCE(nd.max_nonce, t.max_nonce) AS max_nonce
, COALESCE(COALESCE(nd.is_smart_contract, t.is_smart_contract), false) AS is_smart_contract
, COALESCE(nd.namespace, t.namespace) AS namespace
, COALESCE(nd.name, t.name) AS name
, GREATEST(nd.first_funded_by, t.first_funded_by) AS first_funded_by
, COALESCE(t.first_tx_block_time, nd.first_tx_block_time) AS first_tx_block_time
, COALESCE(t.last_tx_block_time, nd.last_tx_block_time) AS last_tx_block_time
, COALESCE(t.first_tx_block_number, nd.first_tx_block_number) AS first_tx_block_number
, COALESCE(t.last_tx_block_number, nd.last_tx_block_number) AS last_tx_block_number
, GREATEST(nd.last_tx_block_time, t.last_seen) AS last_seen
FROM new_data nd
LEFT JOIN {{this}} t ON t.address=nd.address

{% endif %}

{% endmacro %}