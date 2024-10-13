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
    , SUM(COALESCE(tokens_received_count, 0)) AS tokens_received_count
    , SUM(COALESCE(tokens_received_tx_count, 0)) AS tokens_received_tx_count
    , SUM(COALESCE(tokens_sent_count, 0)) AS tokens_sent_count
    , SUM(COALESCE(tokens_sent_tx_count, 0)) AS tokens_sent_tx_count
    , MIN(first_transfer_block_time) AS first_transfer_block_time
    , MAX(last_transfer_block_time) AS last_transfer_block_time
    , MIN(first_received_block_number) AS first_received_block_number
    , MAX(last_received_block_number) AS last_received_block_number
    , MIN(first_sent_block_number) AS first_sent_block_number
    , MAX(last_sent_block_number) AS last_sent_block_number
    , SUM(received_volume_usd) AS received_volume_usd
    , SUM(sent_volume_usd) AS sent_volume_usd
    FROM (
        SELECT "from" AS address
        , 0 AS tokens_received_count
        , 0 AS tokens_received_tx_count
        , COUNT(*) AS tokens_sent_count
        , COUNT(DISTINCT tx_hash) AS tokens_sent_tx_count
        , MIN(block_time) AS first_transfer_block_time
        , MAX(block_time) AS last_transfer_block_time
        , MIN(block_number) AS first_received_block_number
        , MAX(block_number) AS last_received_block_number
        , CAST(NULL AS bigint) AS first_sent_block_number
        , CAST(NULL AS bigint) AS last_sent_block_number
        , 0 AS received_volume_usd
        , SUM(amount_usd) AS sent_volume_usd
        FROM {{token_transfers}}
        GROUP BY "from"

        UNION ALL

        SELECT "to" AS address
        , COUNT(*) AS tokens_received_count
        , COUNT(DISTINCT tx_hash) AS tokens_received_tx_count
        , 0 AS tokens_sent_count
        , 0 AS tokens_sent_tx_count
        , MIN(block_time) AS first_transfer_block_time
        , MAX(block_time) AS last_transfer_block_time
        , CAST(NULL AS bigint) AS first_received_block_number
        , CAST(NULL AS bigint) AS last_received_block_number
        , MIN(block_number) AS first_sent_block_number
        , MAX(block_number) AS last_sent_block_number
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
    , MAX_BY(c.namespace, c.created_at) AS namespace
    , MAX_BY(c.name, c.created_at) AS name
    FROM {{creation_traces}} ct
    LEFT JOIN {{contracts}} c ON ct.address=c.address
    GROUP BY 1
    )

SELECT '{{blockchain}}' AS blockchain
, address
, COALESCE(executed_tx_count, 0) AS executed_tx_count
, max_nonce AS max_nonce
, COALESCE(is_smart_contract, false) AS is_smart_contract
, namespace
, name
, first_funded_by
, ffb.block_time AS first_funded_by_block_time
, tokens_received_count
, tokens_received_tx_count
, tokens_sent_count
, tokens_sent_tx_count
, first_transfer_block_time
, last_transfer_block_time
, first_received_block_number
, last_received_block_number
, first_sent_block_number
, last_sent_block_number
, received_volume_usd
, sent_volume_usd
, first_tx_block_time
, last_tx_block_time
, first_tx_block_number
, last_tx_block_number
, GREATEST(last_tx_block_time, last_transfer_block_time) AS last_seen
, GREATEST(last_tx_block_number, last_received_block_number, last_sent_block_number) AS last_seen_block
FROM transfers
FULL OUTER JOIN executed_txs USING (address)
FULL OUTER JOIN {{ source('addresses_events_'~blockchain, 'first_funded_by')}} ffb USING (address)
FULL OUTER JOIN is_contract ic USING (address)



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
    WHERE (t.address IS NULL OR txs.block_number > t.last_tx_block_number)
    AND {{ incremental_predicate('txs.block_time') }}
    GROUP BY 1
    )


, transfers AS (
    SELECT address
    , SUM(COALESCE(tokens_received_count, 0)) AS tokens_received_count
    , SUM(COALESCE(tokens_received_tx_count, 0)) AS tokens_received_tx_count
    , SUM(COALESCE(tokens_sent_count, 0)) AS tokens_sent_count
    , SUM(COALESCE(tokens_sent_tx_count, 0)) AS tokens_sent_tx_count
    , MIN(first_transfer_block_time) AS first_transfer_block_time
    , MAX(last_transfer_block_time) AS last_transfer_block_time
    , MIN(first_received_block_number) AS first_received_block_number
    , MAX(last_received_block_number) AS last_received_block_number
    , MIN(first_sent_block_number) AS first_sent_block_number
    , MAX(last_sent_block_number) AS last_sent_block_number
    , SUM(received_volume_usd) AS received_volume_usd
    , SUM(sent_volume_usd) AS sent_volume_usd
    FROM (
        SELECT tt."from" AS address
        , 0 AS tokens_received_count
        , 0 AS tokens_received_tx_count
        , COUNT(*) AS tokens_sent_count
        , COUNT(DISTINCT tx_hash) AS tokens_sent_tx_count
        , MIN(tt.block_time) AS first_transfer_block_time
        , MAX(tt.block_time) AS last_transfer_block_time
        , MIN(tt.block_number) AS first_received_block_number
        , MAX(tt.block_number) AS last_received_block_number
        , CAST(NULL AS bigint) AS first_sent_block_number
        , CAST(NULL AS bigint) AS last_sent_block_number
        , 0 AS received_volume_usd
        , SUM(tt.amount_usd) AS sent_volume_usd
        FROM {{token_transfers}} tt
        LEFT JOIN {{this}} t ON tt."from"=t.address
        WHERE (t.address IS NULL OR tt.block_time > t.last_transfer_block_time)
        AND {{ incremental_predicate('tt.block_time') }}
        GROUP BY tt."from"

        UNION ALL

        SELECT tt."to" AS address
        , COUNT(*) AS tokens_received_count
        , COUNT(DISTINCT tx_hash) AS tokens_received_tx_count
        , 0 AS tokens_sent_count
        , 0 AS tokens_sent_tx_count
        , MIN(tt.block_time) AS first_transfer_block_time
        , MAX(tt.block_time) AS last_transfer_block_time
        , CAST(NULL AS bigint) AS first_received_block_number
        , CAST(NULL AS bigint) AS last_received_block_number
        , MIN(tt.block_number) AS first_sent_block_number
        , MAX(tt.block_number) AS last_sent_block_number
        , SUM(tt.amount_usd) AS received_volume_usd
        , 0 AS sent_volume_usd
        FROM {{token_transfers}} tt
        LEFT JOIN {{this}} t ON tt."to"=t.address
        WHERE (t.address IS NULL OR tt.block_time > t.last_transfer_block_time)
        AND {{ incremental_predicate('tt.block_time') }}
        GROUP BY "to"
        )
    GROUP BY 1
    )

, is_contract AS (
    SELECT ct.address
    , true AS is_smart_contract
    , MAX_BY(c.namespace, c.created_at) AS namespace
    , MAX_BY(c.name, c.created_at) AS name
    FROM {{creation_traces}} ct
    LEFT JOIN {{this}} t ON ct.address=t.address
    LEFT JOIN {{contracts}} c ON ct.address=c.address
    WHERE (t.address IS NULL OR ct.block_number > t.last_tx_block_number)
    AND {{ incremental_predicate('ct.block_time') }}
    GROUP BY 1
    )

, new_data AS (
    SELECT address
    , COALESCE(executed_tx_count, 0) AS executed_tx_count
    , max_nonce AS max_nonce
    , is_smart_contract
    , namespace
    , name
    , first_funded_by
    , ffb.block_time AS first_funded_by_block_time
    , tokens_received_count
    , tokens_received_tx_count
    , tokens_sent_count
    , tokens_sent_tx_count
    , first_transfer_block_time
    , last_transfer_block_time
    , first_received_block_number
    , last_received_block_number
    , first_sent_block_number
    , last_sent_block_number
    , received_volume_usd
    , sent_volume_usd
    , first_tx_block_time
    , last_tx_block_time
    , first_tx_block_number
    , last_tx_block_number
    FROM transfers t
    FULL OUTER JOIN executed_txs et ON et.address=t.address
    FULL OUTER JOIN {{ source('addresses_events_'~blockchain, 'first_funded_by')}} ffb ON ffb.address=t.address
        AND {{ incremental_predicate('ffb.block_time') }}
    LEFT JOIN is_contract ic ON ic.address=t.address
    )

SELECT '{{blockchain}}' AS blockchain
, nd.address
, nd.executed_tx_count + t.executed_tx_count AS executed_tx_count
, COALESCE(nd.max_nonce, t.max_nonce) AS max_nonce
, COALESCE(nd.is_smart_contract, t.is_smart_contract) AS is_smart_contract
, COALESCE(nd.namespace, t.namespace) AS namespace
, COALESCE(nd.name, t.name) AS name
, COALESCE(t.first_funded_by, nd.first_funded_by) AS first_funded_by
, COALESCE(t.first_funded_by_block_time, nd.first_funded_by_block_time) AS first_funded_by_block_time
, COALESCE(nd.tokens_received_count, 0)+COALESCE(t.tokens_received_count, 0) AS tokens_received_count
, COALESCE(nd.tokens_received_tx_count, 0)+COALESCE(t.tokens_received_tx_count, 0) AS tokens_received_tx_count
, COALESCE(nd.tokens_sent_count, 0)+COALESCE(t.tokens_sent_count, 0) AS tokens_sent_count
, COALESCE(nd.tokens_sent_tx_count, 0)+COALESCE(t.tokens_sent_tx_count, 0) AS tokens_sent_tx_count
, COALESCE(t.first_transfer_block_time, nd.first_transfer_block_time) AS first_transfer_block_time
, COALESCE(nd.last_transfer_block_time, t.last_transfer_block_time) AS last_transfer_block_time
, COALESCE(t.first_received_block_number, nd.first_received_block_number) AS first_received_block_number
, COALESCE(nd.last_received_block_number, t.last_received_block_number) AS last_received_block_number
, COALESCE(t.first_sent_block_number, nd.first_sent_block_number) AS first_sent_block_number
, COALESCE(nd.last_sent_block_number, t.last_sent_block_number) AS last_sent_block_number
, COALESCE(nd.received_volume_usd, 0)+COALESCE(t.received_volume_usd, 0) AS received_volume_usd
, COALESCE(nd.sent_volume_usd, 0)+COALESCE(t.sent_volume_usd, 0) AS sent_volume_usd
, COALESCE(t.first_tx_block_time, nd.first_tx_block_time) AS first_tx_block_time
, COALESCE(nd.last_tx_block_time, t.last_tx_block_time) AS last_tx_block_time
, COALESCE(t.first_tx_block_number, nd.first_tx_block_number) AS first_tx_block_number
, COALESCE(nd.last_tx_block_number, t.last_tx_block_number) AS last_tx_block_number
, GREATEST(nd.last_tx_block_time, nd.last_transfer_block_time, t.last_seen) AS last_seen
, GREATEST(nd.last_tx_block_number, nd.last_received_block_number, nd.last_sent_block_number, t.last_seen_block) AS last_seen_block
FROM new_data nd
LEFT JOIN {{this}} t ON t.address=nd.address

{% endif %}

{% endmacro %}