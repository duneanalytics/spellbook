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

, fungible_received AS (
    SELECT to AS address
    , MIN(block_time) AS first_received_block_time
    , MAX(block_time) AS last_received_block_time
    , MIN(block_number) AS first_received_block_number
    , MAX(block_number) AS last_received_block_number
    FROM {{token_transfers}}
    GROUP BY 1
    )

, fungible_sent AS (
    SELECT "from" AS address
    , COUNT(DISTINCT tx_hash) AS sent_tx_count
    , MIN(block_time) AS first_sent_block_time
    , MAX(block_time) AS last_sent_block_time
    , MIN(block_number) AS first_sent_block_number
    , MAX(block_number) AS last_sent_block_number
    FROM {{token_transfers}}
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
, first_received_block_time AS first_received_block_time
, first_received_block_number AS first_received_block_number
, GREATEST(last_sent_block_time, last_received_block_time) AS last_transfer_block_time
, GREATEST(last_sent_block_number, last_received_block_number) AS last_transfer_block_number
, GREATEST(GREATEST(last_sent_block_time, last_received_block_time), last_tx_block_time) AS last_updated
FROM executed_txs
LEFT JOIN fungible_received USING (address)
LEFT JOIN fungible_sent USING (address)
LEFT JOIN is_contract USING (address)
LEFT JOIN addresses_events_ethereum.first_funded_by USING (address)



{% else %}



WITH executed_txs AS (
    SELECT txs."from" AS address
    , COUNT(txs.*)+t.executed_tx_count AS executed_tx_count
    , COALESCE(MAX(txs.nonce), 0) AS max_nonce
    , COALESCE(t.first_tx_block_time, MIN(txs.block_time)) AS first_tx_block_time
    , MAX(txs.block_time) AS last_tx_block_time
    , COALESCE(t.first_tx_block_number, MIN(txs.block_number)) AS first_tx_block_number
    , MAX(txs.block_number) AS last_tx_block_number
    FROM {{transactions}} txs
    LEFT JOIN {{this}} t ON txs."from"=t.address
        AND txs.block_number>t.last_tx_block_number
    WHERE {{ incremental_predicate('txs.block_time') }}
    GROUP BY 1
    )

, fungible_received AS (
    SELECT tr.to AS address
    , COALESCE(t.first_received_block_time, MIN(tr.block_time)) AS first_received_block_time
    , MAX(tr.block_time) AS last_received_block_time
    , COALESCE(t.first_received_block_number, MIN(tr.block_number)) AS first_received_block_number
    , MAX(tr.block_number) AS last_received_block_number
    FROM {{token_transfers}} tr
    LEFT JOIN {{this}} t ON tr."from"=t.address
        AND tr.block_number>t.last_tx_block_number
    WHERE {{ incremental_predicate('tr.block_time') }}
    GROUP BY 1
    )

, fungible_sent AS (
    SELECT tr."from" AS address
    , COUNT(DISTINCT tr.tx_hash) + t.sent_tx_count AS sent_tx_count
    , COALESCE(t.first_sent_block_time, MIN(tr.block_time)) AS first_sent_block_time
    , MAX(tr.block_time) AS last_sent_block_time
    , COALESCE(t.first_sent_block_number, MIN(tr.block_number)) AS first_sent_block_number
    , MAX(tr.block_number) AS last_sent_block_number
    FROM {{token_transfers}} tr
    LEFT JOIN {{this}} t ON tr."from"=t.address
        AND tr.block_number>t.last_tx_block_number
    WHERE {{ incremental_predicate('tr.block_time') }}
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
    , first_received_block_time AS first_received_block_time
    , first_received_block_number AS first_received_block_number
    , GREATEST(last_sent_block_time, last_received_block_time) AS last_transfer_block_time
    , GREATEST(last_sent_block_number, last_received_block_number) AS last_transfer_block_number
    FROM executed_txs
    LEFT JOIN fungible_received USING (address)
    LEFT JOIN fungible_sent USING (address)
    LEFT JOIN is_contract USING (address)
    LEFT JOIN addresses_events_ethereum.first_funded_by USING (address)
    )

SELECT nd.address
, COALESCE(nd.executed_tx_count, t.executed_tx_count) AS executed_tx_count
, COALESCE(nd.max_nonce, t.max_nonce) AS max_nonce
, COALESCE(COALESCE(nd.is_smart_contract, t.is_smart_contract), false) AS is_smart_contract
, COALESCE(nd.namespace, t.namespace) AS namespace
, COALESCE(nd.name, t.name) AS name
, COALESCE(t.first_funded_by, nd.first_funded_by) AS first_funded_by
, COALESCE(t.first_tx_block_time, nd.first_tx_block_time) AS first_tx_block_time
, COALESCE(nd.last_tx_block_time, t.last_tx_block_time) AS last_tx_block_time
, COALESCE(t.first_tx_block_number, nd.first_tx_block_number) AS first_tx_block_number
, COALESCE(nd.last_tx_block_number, t.last_tx_block_number) AS last_tx_block_number
, COALESCE(t.first_received_block_time, nd.first_received_block_time) AS first_received_block_time
, COALESCE(t.first_received_block_number, nd.first_received_block_number) AS first_received_block_number
, COALESCE(nd.last_transfer_block_time, t.last_transfer_block_time) AS last_transfer_block_time
, COALESCE(nd.last_transfer_block_number, t.last_transfer_block_number) AS last_transfer_block_number
, GREATEST(COALESCE(nd.last_transfer_block_number, t.last_transfer_block_number), COALESCE(nd.last_tx_block_time, t.last_tx_block_time)) AS last_updated
FROM new_data nd
LEFT JOIN {{this}} t ON t.address=nd.address

{% endif %}

{% endmacro %}