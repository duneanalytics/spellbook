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

, transfers AS (
    SELECT address
    , SUM(received_count) AS received_countxt
    , SUM(sent_count) AS sent_count
    , MAX(first_received_block_time) AS first_received_block_time
    , MAX(last_received_block_time) AS last_received_block_time
    , MAX(first_sent_block_time) AS first_sent_block_time
    , MAX(last_sent_block_time) AS last_sent_block_time
    , MAX(received_volume_usd) AS received_volume_usd
    , MAX(sent_volume_usd) AS sent_volume_usd
    FROM (
        SELECT "from" AS address
        , 0 AS received_count
        , COUNT(*) AS sent_count
        , MIN(block_time) AS first_received_block_time
        , MAX(block_time) AS last_received_block_time
        , CAST(NULL AS timestamp) AS first_sent_block_time
        , CAST(NULL AS timestamp) AS last_sent_block_time
        , 0 AS received_volume_usd
        , SUM(amount_usd) AS sent_volume_usd
        FROM {{token_transfers}}
        GROUP BY "from"

        UNION ALL

        SELECT "to" AS address
        , COUNT(*) AS received_count
        , 0 AS sent_count
        , CAST(NULL AS timestamp) AS first_received_block_time
        , CAST(NULL AS timestamp) AS last_received_block_time
        , MIN(block_time) AS first_sent_block_time
        , MAX(block_time) AS last_sent_block_time
        , SUM(amount_usd) AS received_volume_usd
        , 0 AS sent_volume_usd
        FROM {{token_transfers}}
        GROUP BY "to"
        )
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
, received_count
, sent_count
, first_received_block_time
, last_received_block_time
, first_sent_block_time
, last_sent_block_time
, received_volume_usd
, sent_volume_usd
, first_tx_block_time
, last_tx_block_time
, first_tx_block_number
, last_tx_block_number
, last_tx_block_time AS last_seen
FROM executed_txs
LEFT JOIN is_contract USING (address)
LEFT JOIN transfers USING (address)
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


, transfers AS (
    SELECT address
    , SUM(received_count) AS received_count
    , SUM(sent_count) AS sent_count
    , MAX(first_received_block_time) AS first_received_block_time
    , MAX(last_received_block_time) AS last_received_block_time
    , MAX(first_sent_block_time) AS first_sent_block_time
    , MAX(last_sent_block_time) AS last_sent_block_time
    , MAX(received_volume_usd) AS received_volume_usd
    , MAX(sent_volume_usd) AS sent_volume_usd
    FROM (
        SELECT tt."from" AS address
        , 0 AS received_count
        , COUNT(*) AS sent_count
        , MIN(tt.block_time) AS first_received_block_time
        , MAX(tt.block_time) AS last_received_block_time
        , CAST(NULL AS timestamp) AS first_sent_block_time
        , CAST(NULL AS timestamp) AS last_sent_block_time
        , 0 AS received_volume_usd
        , SUM(tt.amount_usd) AS sent_volume_usd
        FROM {{token_transfers}} tt
        LEFT JOIN {{this}} t ON tt."from"=t.address
            AND tt.block_number>t.last_sent_block_time
        WHERE {{ incremental_predicate('tt.block_time') }}
        GROUP BY tt."from"

        UNION ALL

        SELECT tt."to" AS address
        , COUNT(*) AS received_count
        , 0 AS sent_count
        , CAST(NULL AS timestamp) AS first_received_block_time
        , CAST(NULL AS timestamp) AS last_received_block_time
        , MIN(tt.block_time) AS first_sent_block_time
        , MAX(tt.block_time) AS last_sent_block_time
        , SUM(tt.amount_usd) AS received_volume_usd
        , 0 AS sent_volume_usd
        FROM {{token_transfers}} tt
        LEFT JOIN {{this}} t ON tt."to"=t.address
            AND tt.block_number>t.last_received_block_time
        WHERE {{ incremental_predicate('tt.block_time') }}
        GROUP BY "to"
        )
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
, n.received_count+t.received_count AS received_count
, n.sent_count+t.sent_count AS sent_count
, COALESCE(t.first_received_block_time, n.first_received_block_time) AS first_received_block_time
, COALESCE(n.last_received_block_time, t.last_received_block_time) AS last_received_block_time
, COALESCE(t.first_sent_block_time, n.first_sent_block_time) AS first_sent_block_time
, COALESCE(n.last_sent_block_time, t.last_sent_block_time) AS last_sent_block_time
, n.received_volume_usd+t.received_volume_usd AS received_volume_usd
, n.sent_volume_usd+t.sent_volume_usd AS sent_volume_usd
, COALESCE(t.first_tx_block_time, nd.first_tx_block_time) AS first_tx_block_time
, COALESCE(t.last_tx_block_time, nd.last_tx_block_time) AS last_tx_block_time
, COALESCE(t.first_tx_block_number, nd.first_tx_block_number) AS first_tx_block_number
, COALESCE(t.last_tx_block_number, nd.last_tx_block_number) AS last_tx_block_number
, GREATEST(nd.last_tx_block_time, t.last_seen) AS last_seen
FROM new_data nd
LEFT JOIN {{this}} t ON t.address=nd.address

{% endif %}

{% endmacro %}